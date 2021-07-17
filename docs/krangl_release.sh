# krangl release automation

# adjust to te path of your working copy
#export KRANGL_HOME=/mnt/hgfs/sharedDB/db_projects/krangl/
export KRANGL_HOME=/d/projects/misc/krangl

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
#export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64

git config  user.email "holgerbrandl@users.noreply.github.com"

########################################################################
## Rebuild the javadoc

./gradlew clean dokka

git rm -r --cached ${KRANGL_HOME}/docs/userguide/docs/javadoc

rm -rf ${KRANGL_HOME}/docs/userguide/docs/javadoc
cp -r ${KRANGL_HOME}/javadoc ${KRANGL_HOME}/docs/userguide/docs/javadoc
git add -A ${KRANGL_HOME}/docs/userguide/docs/javadoc

git commit -m "updated javadoc for ${krangl_version} release"

#########################################################################
## Push and wait for travis CI results

# Note: From here on the steps can only be executed by the repo maintainer(s)

kscript src/test/kotlin/krangl/misc/PatchVersion.kts "${krangl_version:1}"

git status
git commit -am "${krangl_version} release"


# make sure that are no pending chanes
#(git diff --exit-code && git tag v${kscript_version})  || echo "could not tag current branch"
git diff --exit-code  || echo "There are uncomitted changes"


git tag "${krangl_version}"

git push origin
git push origin --tags


########################################################################
### Build and publish the binary release to maven-central

#./gradlew install

# careful with this one!
# https://getstream.io/blog/publishing-libraries-to-mavencentral-2021/
# https://central.sonatype.org/pages/gradle.html
 ./gradlew publishToSonatype closeAndReleaseSonatypeStagingRepository

## also see https://oss.sonatype.org/

#For released versions check:
#
#- https://bintray.com/holgerbrandl/mpicbg-scicomp/krangl
#

#4. Increment version to *-SNAPSHOT for next release cycle

