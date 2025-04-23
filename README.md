# This is the 3rd sub repo under graphVCS project
Contains the core logic behind the system 

# relevant files 
vcs.py:
  This file contains the core logic behind the version control system
  This file along with the commit_config.json file, work together
  The commit_config.json file holds the last made commit in the version control system as a field in the json file
  Users can download this script and use my ip address on port 3000 for the react interface for the application
  They'll need access to the s3 buckets, via my roles setup on aws and then merely visiting the <ip>:3000 should open up the graphVCS for you

  neo4j credentials -> my local neo4j instance 
  
  aws credentials -> personal account
    
  # COMPONENTS:
  
  its a functional git style basic version control system with visualizations via neo4j graph databases
  we can perform the following operations with this VCS:
  
  commit -> git commit
  branch -> git checkout
  merge -> git merge
  revert -> git revert 

  

  # SAMPLE COMMAND USE: (you could create a batch file and add to a directory specified in the env. variables path)

  
  to create a new commit 
  # python vcs.py commit commit_id_123 "Initial commit" user1 /path/to/project/directory --branch_name batch

  to create a branch from a particular commit
  # python vcs.py branch "feature_branch" --commit_id commit_id_123

  to switch to a branch (existing)
  # python vcs.py branch "feature_branch"

  to revert to older commit in the new path specified
  # python vcs.py revert commit_id_123 /path/to/restore/directory

  merging two branches, creates a new merge commit as well
  # python vcs.py merge "feature_branch" "master" user1 /path/to/merge/directory --message "Merging feature_branch into master"

  ![image](https://github.com/user-attachments/assets/b8f575e1-8b6c-4410-b898-0581abc85340)  this is the parser used in the script


  commit_config.json is the JSON used for storing the latest commit made to the system
