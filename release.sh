#!/bin/bash

# Preamble

PROGNAME=$(basename $0)

function error_exit
{
	echo "${PROGNAME}: ${1:-"Unknown Error"}" 1>&2
	exit ${2:-1}
}

# Argument validation

[[ -z "$1" ]] && error_exit "Version number must be provided" 1
NVER=$1

# Go to the build directory

BUILDDIR=bocas-parent
TAGBASE=bocas

cd $(dirname $0)/$BUILDDIR || error_exit "Unable to change to build directory $BUILDDIR" 2
[[ -f pom.xml ]] || error_exit "POM not found in build directory $BUILDDIR" 3

# Set version number
mvn -DnewVersion=$NVER versions:set || error_exit "Unable to set version to $NVER" 4
# Verify build
mvn -Psonatype-oss-release clean verify || error_exit "Verify build failed" 5
# Commit
git commit -a -m "Version $NVER" || error_exit "Unable to commit" 6
# Install and deploy
mvn -Psonatype-oss-release install deploy || error_exit "Unable to install and deploy version $NVER" 7
# Tag and push
git tag -a $TAGBASE-$NVER -m "Version $NVER" || error_exit "Unable to tag version $NVER" 8
git push --follow-tags || error_exit "Unable push tag version $NVER" 10

