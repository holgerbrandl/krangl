//@file:DependsOnMaven("de.mpicbg.scicomp:krangl:0.7")
@file:DependsOnMaven("com.github.holgerbrandl.krangl:krangl-beakerx:1.0-SNAPSHOT")

import krangl.*
import krangl.beakerx.TableDisplayer

irisData.glimpse()


// use table widget
krangl.beakerx.TableDisplayer.register()