#!/usr/bin/env bash

die() { echo "$*" 1>&2; exit; }

# This script only works if it's run from the root of the source tree.
cd build/generated-client/ || die Error: must run ${0} from the root of the source tree

RELEASE_NOTE=${1:-Minor update}

# Initialize the local directory as a Git repository
git init

# Adds the files in the local repository and stages them for commit.
git add .

# Set the origin
git remote add origin git@github.com:CancerDataAggregator/cda-service-python-client.git

# Commits the tracked changes and prepares them to be pushed to a remote repository.
git commit -m "$RELEASE_NOTE"

# checkout master
git fetch origin

# Rebase our local changes on master, chosing our changes over master
git rebase -X theirs origin/master

# Pushes (Forces) the changes in the local repository up to the remote repository
git push --force origin master 2>&1
