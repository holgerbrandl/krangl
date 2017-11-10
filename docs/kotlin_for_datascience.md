
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


different way to specify engines ```{r engine='python'} -> ```{python} #963 https://github.com/yihui/knitr/issues/963


Possible Workarounds:

kscript -> convertnb with jupyter

extract code-chunks into kscriptlet --> convertnb with jupyter


https://github.com/aaren/notedown


```bash
#pip install --user https://github.com/aaren/notedown/tarball/master

## use branch that fixes https://github.com/aaren/notedown/pull/32
pip install --user  https://github.com/aaren/notedown/tarball/kernelspec

export PATH=~/.local/bin:${PATH} 
cd /Users/brandl/projects/kotlin/krangl/docs
notedown --help
notedown /Users/brandl/projects/kotlin/krangl/docs/notebook_example.md > output.ipynb

## for github readme rendering see https://github.com/aaren/notedown
notedown with_output_cells.md --to markdown --strip > no_output_cells.md

notedown input.ipynb --kernel kotlin --to markdown > output_with_outputs.md



## or run it right away
## not supported yet
#notedown notebook.md --run > executed_notebook.ipynb 

nbconvert executed_notebook.ipynb markdown again

conda install jupyter

https://stackoverflow.com/questions/13916820/how-to-install-a-specific-version-of-a-package-with-pip

conda update --all
```


notedown doesn't work with Jupter Notebook 5.1.0 #60
https://github.com/aaren/notedown/issues/60
jupyter notebook --version
5.1.0

https://github.com/jupyter/notebook/issues/2798


Next https://github.com/jupyter/nbconvert