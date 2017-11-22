
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

final setup

https://stackoverflow.com/questions/13916820/how-to-install-a-specific-version-of-a-package-with-pip

```
sudo pip3 uninstall notebook
sudo pip3 install notebook==5.0.0
sudo pip3 install notebook==5.2.1
pip install --user  https://github.com/aaren/notedown/tarball/kernelspec
notedown --help

jupyter notebook --help
jupyter notebook 

```

notedown doesn't work with Jupter Notebook 5.1.0 #60
https://github.com/aaren/notedown/issues/60
jupyter notebook --version
5.1.0

https://github.com/jupyter/notebook/issues/2798




```bash
#pip install --user https://github.com/aaren/notedown/tarball/master

## use branch that fixes https://github.com/aaren/notedown/pull/32
pip install --user  https://github.com/aaren/notedown/tarball/kernelspec

export PATH=~/.local/bin:${PATH} 
cd /Users/brandl/projects/kotlin/krangl/docs/notedown_playground
notedown --help
notedown example_readme.md > output.ipynb


## python example
cat python_readme.md
notedown --run --kernel python3 python_readme.md > python_readme.ipynb
cat python_readme.ipynb
notedown python_readme.ipynb --to markdown  --render > python_readme.with_outputs.md
cat python_readme.with_outputs.md


## basic kotlin example
notedown --run --kernel kotlin example_readme.md > example_readme.ipynb
notedown --to markdown example_readme.ipynb > example_readme.with_outputs.md

# or in one go
notedown --run --kernel kotlin --to markdown --render example_readme.md > example_readme.with_outputs.md


## with images
notedown --run --kernel kotlin --to markdown --render image_example_readme.md > image_example_readme.with_outputs.md

#grep_header() {
#cat - | kscript -t 'lines.filter{ !it.startsWith("RESPONSE") }.print()' 
#}
grep_header() {
cat - | kscript -t 'lines.filter{ !it.startsWith("RESPONSE") }.print()' /dev/stdin  
}
grep_header() {
cat - | grep -v RESPONSE | grep -v "[[]mai" 
}

#echo RESPONSE |  kscript -t 'lines.filter{ !it.startsWith("RESPONSE") }.print()'
echo RESPONSE |  grep_header
echo foo |  grep_header

notedown --run --kernel kotlin image_example_readme.md 2>/dev/null |\
  grep_header |\
  kscript -t 'lines.dropWhile { !it.startsWith("{")}.print()' /dev/stdin |\
   tee image_example_readme.ipynb

rm image_example_readme.with_outputs.md
notedown --to markdown image_example_readme.ipynb >  image_example_readme.with_outputs.md
jupyter nbconvert image_example_readme.ipynb
head  image_example_readme.with_outputs.md
open image_example_readme.with_outputs.md


notedown --to markdown /Users/brandl/Desktop/jup5.2.ipynb > jup5.with_outputs.md


m

```

# nbconvert

```bash
cd /Users/brandl/projects/kotlin/krangl/docs/notedown_playground
notedown image_example_readme.md > foo.ipynb
jupyter nbconvert --execute --to markdown foo.ipynb --stdout > foo.rendered.md


or just run it with
jupyter nbconvert --execute foo.ipynb
notedown --to markdown /Users/brandl/Desktop/jup5.2.ipynb > jup5.with_outputs.md


## works
jupyter nbconvert --execute --to notebook foo.ipynb
jupyter nbconvert --execute --to markdown foo.ipynb
jupyter nbconvert --to markdown foo.ipynb

notedown --to markdown --render foo.nbconvert.ipynb  > foo.convert.md 

```


# final workflow
```bash
#notedown --kernel kotlin image_example_readme.md > foo.ipynb
notedown  image_example_readme.md > foo.ipynb

#http://nbconvert.readthedocs.io/en/latest/execute_api.html
jupyter nbconvert --ExecutePreprocessor.kernel_name=kotlin --execute --to markdown foo.ipynb

```


Next https://github.com/jupyter/nbconvert



## kernel bug

should be
```
},
     "metadata": {}
     "output_type": "display_data"
    }
   ],
   "source": [
```

but is
```
},
     "metadata": {}
    }
   ],
   "source": [
```