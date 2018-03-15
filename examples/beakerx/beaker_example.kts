//@file:DependsOnMaven("de.mpicbg.scicomp:krangl:0.7")
@file:DependsOnMaven("com.github.holgerbrandl.krangl:krangl-beakerx:1.0-SNAPSHOT")
//@file:DependsOnMaven("com.github.holgerbrandl.krangl:krangl-beakerx:1.0-SNAPSHOT")

//%classpath add mvn com.github.holgerbrandl.krangl krangl-beakerx 1.0-SNAPSHOT
//%classpath add mvn de.mpicbg.scicomp krangl 0.7
//%classpath add mvn de.mpicbg.scicomp krangl 0.8-SNAPSHOT

// works:
//%classpath add mvn org.apache.commons commons-csv 1.3

import krangl.*
import krangl.beakerx.TableDisplayer

krangl.beakerx.TableDisplayer.register()

irisData.glimpse()


// use table widget
