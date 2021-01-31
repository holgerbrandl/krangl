# krangl release automation

# adjust to te path of your working copy
#export KRANGL_HOME=/mnt/hgfs/sharedDB/db_projects/krangl/
export KRANGL_HOME=/c/Users/brandl/Dropbox/sharedDB/db_projects/krangl

#########################################################################
## Update release notes in [CHANGES.md](../CHANGES.md)


#########################################################################
## Run tests locally

cd $KRANGL_HOME

./gradlew check

########################################################################
## Increment version in readme, gradle, example-poms and

#**{todo}** automate this

## **{todo}** fix issues with EOL in VM

trim() { while read -r line; do echo "$line"; done; }
krangl_version='v'$(grep '^version' ${KRANGL_HOME}/build.gradle | cut -f2 -d' ' | tr -d "'" | trim)

echo "new version is $krangl_version"



if [[ $krangl_version == *"-SNAPSHOT" ]]; then
  echo "ERROR: Won't publish snapshot build $krangl_version!" 1>&2
  exit 1
fi

#change to use java8 for building because of https://github.com/Kotlin/dokka/issues/294
# update-java-alternatives -l
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64


########################################################################
## Rebuild the javadoc

./gradlew clean dokka

git rm -r --cached ${KRANGL_HOME}/docs/userguide/docs/javadoc

rm -rf ${KRANGL_HOME}/docs/userguide/docs/javadoc
cp -r ${KRANGL_HOME}/javadoc ${KRANGL_HOME}/docs/userguide/docs/javadoc
git add -A ${KRANGL_HOME}/docs/userguide/docs/javadoc

git commit -m "updated javadoc for v${krangl_version} release"

#########################################################################
## Push and wait for travis CI results

# Note: From here on the steps can only be executed by the repo maintainer(s)

# make sure that are no pending chanes
#(git diff --exit-code && git tag v${kscript_version})  || echo "could not tag current branch"
git diff --exit-code  || echo "There are uncomitted changes"

git push

#########################################################################
#5. Do the release

# dokka related gradle deprecation warnings https://github.com/Kotlin/dokka/issues/515
./gradlew assemble



### Do the github release
## see https://github.com/aktau/github-release

## create tag on github 
#github-release --help

#source /Users/brandl/archive/gh_token.sh
source ~/archive/gh_token.sh
export GITHUB_TOKEN=${GH_TOKEN}
#echo $GITHUB_TOKEN

# make your tag and upload
cd ${KRANGL_HOME}

git config  user.email "holgerbrandl@users.noreply.github.com"


#git tag v${krangl_version} && git push --tags
(git diff --ignore-submodules --exit-code && git tag "v${krangl_version}")  || echo "could not tag current branch"
# -> no longer needed because of github-release

git push --tags

# check the current tags and existing releases of the repo
# binaries are located under $GOPATH/bin
export PATH=~/go/bin/:$PATH

github-release info -u holgerbrandl -r krangl

# create a formal release
github-release release \
    --user holgerbrandl \
    --repo krangl \
    --tag "v${krangl_version}" \
    --name "v${krangl_version}" \
    --description "See [CHANGES.md](https://github.com/holgerbrandl/krangl/blob/master/CHANGES.md) for changes." 
#    --pre-release


########################################################################
### Build and publish the binary release to jcenter

gradle install

# careful with this one!
gradle bintrayUpload

#For released versions check:
#
#- https://bintray.com/holgerbrandl/mpicbg-scicomp/krangl
#- https://jcenter.bintray.com/de/mpicbg/scicomp/krangl/
#

#4. Increment version to *-SNAPSHOT for next release cycle

