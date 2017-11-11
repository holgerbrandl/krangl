## Release Checklist

1. Increment version in `krangl`
2. Make sure that support api version is up to date and available from jcenter
3. Update the API docs
4. Push and wait for travis CI results

```bash
export KRANGL_HOME="/Users/brandl/projects/kotlin/krangl";

trim() { while read -r line; do echo "$line"; done; }
krangl_version=$(grep '^version' ${KRANGL_HOME}/build.gradle | cut -f2 -d'=' | tr -d "'" | trim)

echo "new version is $krangl_version"
```


### Do the github release

see https://github.com/aktau/github-release

```bash
## create tag on github 
#github-release --help

source /Users/brandl/Dropbox/archive/gh_token.sh
export GITHUB_TOKEN=${GH_TOKEN}
#echo $GITHUB_TOKEN

# make your tag and upload
cd ${KRANGL_HOME}

#git tag v${krangl_version} && git push --tags
(git diff --exit-code && git tag v${krangl_version})  || echo "could not tag current branch"

git push --tags

# check the current tags and existing releases of the repo
# binaries are located under $GOPATH/bin
github-release info -u holgerbrandl -r krangl

# create a formal release
github-release release \
    --user holgerbrandl \
    --repo krangl \
    --tag "v${krangl_version}" \
    --name "v${krangl_version}" \
    --description "See [NEWS.md](https://github.com/holgerbrandl/krangl/blob/master/Changes.md) for changes." 
#    --pre-release

```
### Build and publish the binary release to jcenter


```bash
cd ${KRANGL_HOME}

gradle install
# careful with this one!
#gradle bintrayUpload
```
