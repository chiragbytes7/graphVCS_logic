import shutil
import argparse
from neo4j import GraphDatabase
import datetime
import os
import json
import sys
import boto3
from botocore.exceptions import NoCredentialsError
import io
import stat
import hashlib
from dotenv import load_dotenv

config_path = "commit_config.json"

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

class S3ConnectionSingleton:
    _instance = None
    _client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(S3ConnectionSingleton, cls).__new__(cls)
            cls._instance._client = None
        return cls._instance

    def get_client(self):
        """Get S3 client, creating it if necessary."""
        if self._client is None:
            print("Creating new S3 client...")
            try:
                self._client = boto3.client('s3')
            except NoCredentialsError:
                print("No AWS credentials found.")
        return self._client

class VersionControl:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.latest_commit_id = None
        self.current_branch = "master"
        try:
            with self.driver.session() as session:
                session.run("RETURN 1")
                print("Neo4j connection established successfully!")
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")

    def read_latest_commit(self):
        if os.path.exists(config_path):
            with open(config_path, 'r') as file:
                data = json.load(file)
                return data.get("latest_commit_id")
        else:
            return None
        
    def write_latest_commit(self, commit_id):
        with open(config_path, 'w') as file:
            json.dump({"latest_commit_id": commit_id}, file)
            
    def s3_store(self, s3_client, files, bucket_name="neo4jvcs", base_prefix=""):
        try:
            for rel_path, file_content in files.items():
                file_stream = io.BytesIO(file_content.encode())
                object_key = os.path.join(base_prefix, rel_path).replace("\\", "/")
                s3_client.upload_fileobj(file_stream, bucket_name, object_key)
        except Exception as e:
            print("Some error occurred while storing the commit:", e)

    def create_commit(self, commit_id, message, user_id, files, s3_client, commit_branch):
        timestamp = datetime.datetime.now().isoformat()
        with self.driver.session() as session:
            if commit_branch == "master":
                parent_commit_id = self.read_latest_commit()
            else:
                result = session.run(
                    "MATCH (n:HEAD {branch: $commit_branch})-[:POINTS_TO]->(last_commit:Commit) "
                    "RETURN last_commit.id as commit_id",
                    commit_branch=commit_branch
                )
                record = result.single()
                parent_commit_id = record["commit_id"] if record else None

        with self.driver.session() as session:
            session.run(
                "CREATE (c:Commit {id: $commit_id, message: $message, timestamp: $timestamp})"
                " MERGE (u:User {id: $user_id})"
                " CREATE (c)-[:MADE_BY]->(u)",
                commit_id=commit_id, message=message, timestamp=timestamp, user_id=user_id
            )

            if parent_commit_id:
                session.run(
                    "MATCH (child:Commit {id: $commit_id}), (parent:Commit {id: $parent_commit_id}) "
                    "CREATE (child)-[:PARENT]->(parent)",
                    commit_id=commit_id, parent_commit_id=parent_commit_id
                )
                print(f"Parent commit {parent_commit_id} linked to {commit_id}")

            self.write_latest_commit(commit_id)
            self.update_head(commit_branch, commit_id)
            self.s3_store(s3_client, files, base_prefix=commit_id)

        print(f"Commit created: {commit_id} - {message}")

    def create_branch(self, branch_name, commit_id):
        with self.driver.session() as session:
            session.run(
                "CREATE (b:Branch {name: $branch_name})"
                " MERGE (c:Commit {id: $commit_id})"
                " CREATE (b)-[:BASE]->(c)",
                branch_name=branch_name, commit_id=commit_id
            )

            session.run(
                "MERGE (h:HEAD {branch: $branch_name})"
                " MERGE (c:Commit {id: $commit_id})"
                " CREATE (h)-[:POINTS_TO]->(c)",
                branch_name=branch_name, commit_id=commit_id
            )
            print(f"Branch {branch_name} created from commit {commit_id}")
            
    def branch_exists(self, branch_name):
        with self.driver.session() as session:
            result = session.run(
                "MATCH (b:Branch {name: $branch_name}) RETURN b",
                branch_name=branch_name
            )
            return result.single() is not None

    def update_head(self, branch_name, commit_id):
        with self.driver.session() as session:
            session.run(
                """
                MATCH (h:HEAD {branch: $branch_name})-[r:POINTS_TO]->()
                DELETE r
                WITH h
                MATCH (c:Commit {id: $commit_id})
                CREATE (h)-[:POINTS_TO]->(c)
                """, 
                branch_name=branch_name, commit_id=commit_id
            )
            
    def revert_to_commit(self, commit_id, client, to_be_deleted_dir, bucket_name="neo4jvcs"):
        cwd = to_be_deleted_dir
        commit_prefix = f"{commit_id}/"

        def force_delete(path):
            if os.path.isfile(path) or os.path.islink(path):
                os.chmod(path, stat.S_IWRITE)
                os.unlink(path)
            elif os.path.isdir(path):
                def on_rm_error(func, path, exc_info):
                    os.chmod(path, stat.S_IWRITE)
                    func(path)
                shutil.rmtree(path, onerror=on_rm_error)

        for item in os.listdir(cwd):
            if not item.startswith('.'):
                item_path = os.path.join(cwd, item)
                force_delete(item_path)

        paginator = client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=commit_prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('/'):
                    continue

                relative_path = key[len(commit_prefix):]
                local_path = os.path.normpath(os.path.join(cwd, *relative_path.split('/')))
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                client.download_file(bucket_name, key, local_path)
        
        print(f"Restored commit {commit_id} to {cwd}")
    
    def switch_branch(self, branch_name):
        with self.driver.session() as session:
            result = session.run(
                "MATCH (h:HEAD {branch: $branch_name})-[:POINTS_TO]->(c:Commit) "
                "RETURN c.id AS current_commit_id",
                branch_name=branch_name
            )
            record = result.single()
            if record:
                self.current_branch = branch_name
                self.write_latest_commit(record["current_commit_id"])
                print(f"Switched to branch {branch_name}")
            else:
                print(f"Branch {branch_name} does not exist")

    def create_branch_from_commit(self, branch_name, commit_id):
        self.create_branch(branch_name, commit_id)
        self.switch_branch(branch_name)

    def close(self):
        self.driver.close()

    def read_file_content(self, file_path):
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                return file.read()
        else:
            print(f"File not found: {file_path}")
            return None
    
    def read_directory(self, dir_path):
        collected_files = {}
        for root, dirs, files_in_dir in os.walk(dir_path):
            for file_name in files_in_dir:
                file_path = os.path.join(root, file_name)
                rel_path = os.path.relpath(file_path, dir_path)
                file_content = self.read_file_content(file_path)
                if file_content is not None:
                    collected_files[rel_path.replace("\\", "/")] = file_content
        return collected_files


    def merge_branches(self, source_branch, target_branch, user_id, message, directory_delete):
        #logic to find lca, get all versions, hash em and check rule based 
        s3_client = S3ConnectionSingleton().get_client()
        bucket_name = "neo4jvcs"
        with self.driver.session() as session:
            result = session.run("""
                                 MATCH (n:HEAD {branch: $branch})-[:POINTS_TO]->(c:Commit)
                                 RETURN c.id as commit_id
                                 """, branch = source_branch)
            record = result.single()
            if not record:
                raise ValueError(f"No HEAD found for target branch '{target_branch}'")
            head_for_source_id = record['commit_id']
            result = session.run("""
            MATCH (n:HEAD {branch: $branch})-[:POINTS_TO]->(c:Commit)
                                RETURN c.id as commit_id
                        """, branch = target_branch)
            record = result.single()
            if not record:
                raise ValueError(f"No HEAD found for target branch '{target_branch}'")
            head_for_target_id = record['commit_id']
            
            print("head prints below")
            print(head_for_source_id)
            print(head_for_target_id)
            
            ancestors_for_1 = {}
            ancestors_for_2 = {}
            #we have both the commits , now have to find the LCA
            result = session.run("""
                                 MATCH path = (n:Commit)-[:PARENT*]->(a:Commit)
                                 WHERE n.id = $id
                                 RETURN a.id AS id, length(path) AS distance
                                 """, id = head_for_source_id)
            for record in result:
                commit_id = record['id']
                distance = record['distance']
                ancestors_for_1[commit_id] = distance
                
            result = session.run("""
                                 MATCH path = (n:Commit)-[:PARENT*]->(a:Commit)
                                 WHERE n.id = $id
                                 RETURN a.id AS id, length(path) AS distance
                                 """, id = head_for_target_id)
            for record in result:
                commit_id = record['id']
                distance = record['distance']
                ancestors_for_2[commit_id] = distance
            
            print("below this")
            print("Ancestors for source:", ancestors_for_1)
            print("Ancestors for target:", ancestors_for_2)
            
            ancestors = set(ancestors_for_1.keys()) & set(ancestors_for_2.keys())   
            if not ancestors:
                raise Exception("No common ancestor found between the two branches.")
            
            lca_id = min(ancestors, key=lambda cid:  ancestors_for_1[cid] + ancestors_for_2[cid])
            print(f"{lca_id} is the lca id")
            
            # we now have the lca , as in we have all the 3 commits 
            
            hash_for_ancestor_dir, hash_for_ancestor_files = self.fetch_version_hash(lca_id)             
            hash_for_source_dir, hash_for_source_files = self.fetch_version_hash(head_for_source_id)                      #hash for dir and a list that returns hashes of all the files
            hash_for_target_dir, hash_for_target_files = self.fetch_version_hash(head_for_target_id) 
            
            # 4 types of cases 
            
            if(hash_for_ancestor_dir == hash_for_target_dir and hash_for_ancestor_dir != hash_for_source_dir):            # this means that only source has changed, fast forward merge
                self.revert_to_commit(head_for_source_id, s3_client, directory_delete)
                merge_id = self.merge_commit(head_for_source_id, head_for_target_id, user_id, message)
                self.terminate_branch(source_branch)
                print("new merge commit created")
                self.exit_logging()

            elif(hash_for_ancestor_dir != hash_for_target_dir and hash_for_ancestor_dir == hash_for_source_dir):          # this means that nothing changed in source branch, use the target branch
                self.revert_to_commit(head_for_target_id, s3_client, directory_delete)                                       # nothing chnaged in source, put the target version
                merge_id = self.merge_commit(head_for_source_id, head_for_target_id, user_id, message)
                # self.terminate_branch(source_branch)
                print("new merge commit created")
                self.exit_logging()

            elif(hash_for_ancestor_dir == hash_for_target_dir == hash_for_source_dir):                                    # no changes anywhere , no need to merge
                print("no merge required, nothing to merge")                                                     
                merge_id = self.merge_commit(head_for_source_id, head_for_target_id, user_id, message)      
                # self.terminate_branch(source_branch)
                print("new merge commit created")
                self.exit_logging()

            else:
                raise ValueError("conflicting versions encountered between source and target branches")
            
            self.update_head(target_branch, merge_id)
            
    def exit_logging(self):
        print("exited the merge branches loop")    
            
    def merge_commit(self, commit1, commit2, user_id, message):
        print("entered merge commit function")
        timestamp = datetime.datetime.now().isoformat()
        last_commit_id = self.read_latest_commit() 
        string = last_commit_id[:-1]
        number = last_commit_id[-1]
        merge_commit_id = string + str(int(number) + 1)
        
        with self.driver.session() as session:
            result = session.run("""
                            MERGE (c:Commit {id: $merge_commit_id, message: $message, timestamp: $timestamp})
                            WITH c
                            MATCH (a:Commit {id: $source_id}), (b:Commit {id: $target_id}), (u:User {id: $user_id})
                            CREATE (c)-[:PARENT]->(a), (c)-[:PARENT]->(b)
                            CREATE (c)-[:MADE_BY]->(u)
                                 """, merge_commit_id=merge_commit_id, source_id = commit1, target_id = commit2, user_id = user_id, message = message, timestamp = timestamp)
        self.write_latest_commit(merge_commit_id)
        return merge_commit_id

    def fetch_version_hash(self, commit_id):
        s3_client = S3ConnectionSingleton().get_client()
        bucket_name = "neo4jvcs"
        prefix = commit_id + '/'

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        file_hashes = []

        if 'Contents' in response:
            # Sort files by their relative paths (without commit_id prefix)
            sorted_files = sorted(
                response['Contents'],
                key=lambda x: x['Key'][len(prefix):] if not x['Key'].endswith('/') else ''
            )
            
            for obj in sorted_files:
                key = obj['Key']
                if key.endswith('/'):
                    continue  # skip folder key

                file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
                content = file_obj['Body'].read()
                
                # Normalize line endings to LF
                content = content.replace(b'\r\n', b'\n')
                
                # Get relative path without commit_id prefix
                rel_path = key[len(prefix):]
                
                # Git-style SHA-1: prefix with "blob <size>\0"
                header = f"blob {len(content)}\0".encode('utf-8')
                git_blob = header + content
                
                # Compute SHA-1
                file_sha1 = hashlib.sha1(git_blob).hexdigest()
                file_hashes.append((rel_path, file_sha1))

        # Combine file hashes to get a directory hash
        # We sort by relative path to ensure deterministic ordering
        combined = ''.join(f"{path}:{h}" for path, h in sorted(file_hashes, key=lambda x: x[0])).encode('utf-8')
        directory_sha1 = hashlib.sha1(combined).hexdigest()

        print("File Hashes:")
        for path, h in file_hashes:
            print(f"{path} -> {h}")
        print("Directory Hash:", directory_sha1)

        return directory_sha1, file_hashes

    def terminate_branch(self, branch_name):
        with self.driver.session() as session:
            # First check if branch exists
            result = session.run(
                "MATCH (b:Branch {name: $branch_name}) RETURN b",
                branch_name=branch_name
            )
            if not result.single():
                print(f"Branch {branch_name} does not exist")
                return False
                
            # Delete branch and its HEAD
            session.run("""
                MATCH (b:Branch {name: $branch_name})
                MATCH (h:HEAD {branch: $branch_name})
                DETACH DELETE b, h
            """, branch_name=branch_name)
            
            print(f"Branch {branch_name} has been terminated")
            return True



def parse_args():
    parser = argparse.ArgumentParser(description="A simple version control system")
    subparsers = parser.add_subparsers(dest="command")

    commit_parser = subparsers.add_parser("commit", help="Create a commit")
    commit_parser.add_argument("commit_id", help="Unique ID for the commit")
    commit_parser.add_argument("message", help="Commit message")
    commit_parser.add_argument("user_id", help="User performing the commit")
    commit_parser.add_argument("directory", help="Directory to include in commit")
    commit_parser.add_argument("--branch_name", help="Branch name for commit")

    branch_parser = subparsers.add_parser("branch", help="Create or switch to a branch")
    branch_parser.add_argument("branch_name", help="Branch name")
    branch_parser.add_argument("--commit_id", help="Commit ID to base the branch on (optional)")

    revert_parser = subparsers.add_parser("revert", help="Revert to an older version")
    revert_parser.add_argument("commit_id", help="Commit ID to revert to")
    revert_parser.add_argument("directory", help="Directory to restore content to")
    
    merge_parser = subparsers.add_parser("merge", help="Merge one branch into another")
    merge_parser.add_argument("source_branch", help="Source branch to merge from")
    merge_parser.add_argument("target_branch", help="Target branch to merge into")
    merge_parser.add_argument("user_id", help="User performing the merge")
    merge_parser.add_argument("directory_delete", help="directory to put the merged latest version")
    merge_parser.add_argument("--message", help="Merge commit message")
    
    
    return parser.parse_args()

def main():
    s3_connection = S3ConnectionSingleton()
    client = s3_connection.get_client()
    args = parse_args()
    vc = VersionControl(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)

    if len(sys.argv) == 1:
        if not (vc.branch_exists(vc.current_branch)):
            vc.create_branch(vc.current_branch, "root_commit")
            vc.latest_commit_id = "root_commit"
            vc.write_latest_commit(vc.latest_commit_id)
            
    elif args.command == "commit":
        files = vc.read_directory(args.directory)
        branch = args.branch_name if args.branch_name else "master"
        vc.create_commit(args.commit_id, args.message, args.user_id, files, client, branch)
    elif args.command == "branch":
        if args.commit_id:
            vc.create_branch_from_commit(args.branch_name, args.commit_id)
        else:
            vc.switch_branch(args.branch_name)
    elif args.command == "revert":
        vc.revert_to_commit(args.commit_id, client, args.directory)
    elif args.command == "merge":
        try:
            # merge_commit_id = vc.merge_branches(
            #     args.source_branch,
            #     args.target_branch,
            #     args.user_id,
            #     args.message
            # )
            vc.merge_branches(args.source_branch,args.target_branch,args.user_id,args.message, args.directory_delete)
            print(f"Successfully merged {args.source_branch} into {args.target_branch}")
            
        except ValueError as e:
            print(f"Merge failed: {str(e)}")
    else:
        vc.switch_branch(args.branch_name)

    vc.close()

if __name__ == "__main__":
    main()



