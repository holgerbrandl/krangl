# Developer Notes for Krangl



# Interactive shell
```bash
kscript -i - <<"EOF"
//DEPS de.mpicbg.scicomp:krangl:0.9-SNAPSHOT
EOF
```

## Potentially useful libraries

* https://github.com/mplatvoet/progress
* https://github.com/SalomonBrys/Kodein cool dependency injection
* https://github.com/hotchemi/khronos date extension
* https://github.com/zeroturnaround/zt-exec cool process builder api


## Design

https://stackoverflow.com/questions/45090808/intarray-vs-arrayint-in-kotlin --> bottom line: Array<*> can be null


## Gradle

create fresh gradle wrapper with:

`gradle wrapper --gradle-version 4.2.1`

From https://github.com/twosigma/beakerx/issues/5135: Split repos?
> It is a bad idea. Many different repos are hard to maintain. And you do not need this. Gradle allows to publish separate artifacts without splitting repository.  
you can use `gradle :kernel:base:<whatever>` instead of `cd`.




## project documentation


```bash
hugo new site quickstart
cd quickstart

git init
git submodule add https://github.com/matcornic/hugo-theme-learn.git themes/hugo-theme-learn
 

# Edit your config.toml configuration file # and add the Ananke theme. 
#echo 'theme = "ananke"' >> config.toml

hugo serve -t hugo-theme-learn
hugo serve -t hugo-default-theme
hugo serve -t natrium
hugo serve -t kraiklyn

hugo new content/introduction/Quickstart.md





https://stackoverflow.com/questions/43555696/why-is-hugo-serving-blank-pages

```

star icon -> https://gohugo.io/documentation/



### gitbook documentation

https://github.com/GitbookIO/gitbook/blob/master/docs/setup.md

```bash
npm install gitbook-cli -g

gitbook serve
gitbook build

gitbook build ./ --log=debug --debug

```

don't miss the local editor
https://www.gitbook.com/editor

https://github.com/GitbookIO/theme-api


# Reading Log & Misc


From spark release notes:
> Unifying DataFrames and Datasets in Scala/Java: Starting in Spark 2.0, DataFrame is just a type alias for Dataset of Row. Both the typed methods (e.g. map, filter, groupByKey) and the untyped methods (e.g. select, groupBy) are available on the Dataset class. Also, this new combined Dataset interface is the abstraction used for Structured Streaming. Since compile-time type-safety in Python and R is not a language feature, the concept of Dataset does not apply to these languages’ APIs. Instead, DataFrame remains the primary programing abstraction, which is analogous to the single-node data frame notion in these languages. Get a peek from a Dataset API notebook.

---

http://stackoverflow.com/questions/29268526/how-to-overcome-same-jvm-signature-error-when-implementing-a-java-interface

To Improve JVM compatibility use JvmName to allow for more strongly typed

```kotlin
@JvmName("mutateString")
fun DataFrame.mutate(name: String, formula: (DataFrame) -> List<String>): DataFrame {
    if(this is SimpleDataFrame){
        return addColumn(StringCol(name, formula(this)))
    }else
        throw UnsupportedOperationException()
}

```