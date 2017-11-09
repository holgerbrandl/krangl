
### IDE issues

Send to REPL https://youtrack.jetbrains.com/issue/KT-11409

macro recorder might help

Misc
* https://youtrack.jetbrains.com/issue/KT-7100
* https://youtrack.jetbrains.com/issue/KT-13319
* https://youtrack.jetbrains.com/issue/KT-14177
* https://youtrack.jetbrains.com/issue/KT-14851
* https://youtrack.jetbrains.com/issue/KT-21152



## Plotting

there is already https://github.com/kristapsdz/kplot

API ideas
```kotlin

## dont
plotOf(df).aes{ x=}.addLines().add

# do
plotOf(df).mapAesthetics{ x="it", }.addLines().facetGrid(freeX=tue){}


```


## Report rendering

https://github.com/kategory/ank

Example: https://github.com/kategory/ank/tree/master/sample