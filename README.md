# jupyter_drill
A module to help interaction with Jupyter Notebooks and Apache Drill

------
This is a python module that helps to connect Jupyter Notebooks to various datasets. 
It's based on (and requires) https://github.com/JohnOmernik/jupyter_integration_base 





## Initialization 
----
After installing this, to instantiate the module so you can use %drill and %%drill put this in a cell:

the Drill() init can take the following arguments, or they can be left off to use these defaults

 pd_display_grid="html", drill_rewrite_host=False, drill_pin_to_ip=False, drill_embedded=False

- pd_display_grid
  - html - Use standard html display in data frames (default)
  - qgrid - Use qgrid (https://github.com/quantopian/qgrid v. 1.3 or higher) Recommended 
- drill_rewrite_host
  - This is an obscure work around when you have a drill cluster behind a load balancer.  Leave off or leave as false
- drill_pin_to_ip
  - This is a obscure work around when you have a drill cluster behind a load balancer.
- drill_embedded
  - Set this to True to connect to a localhost hosted drill bit on port 8047. It doesn't require any variables/passwords.



### Example Inits

#### Embedded mode using qgrid
```
from drill_core import Drill
ipy = get_ipython()
Drill = Drill(ipy, drill_embedded=True,  pd_display_grid="qgrid")
ipy.register_magics(Drill)
```

#### Cluster Mode using html display
---
Both of the following do the same thing


Explicit

```
from drill_core import Drill
ipy = get_ipython()
Drill = Drill(ipy, drill_embedded=False,  pd_display_grid="html")
ipy.register_magics(Drill)
```


Implicit

```
from drill_core import Drill
ipy = get_ipython()
Drill = Drill(ipy)
ipy.register_magics(Drill)
```

## ENV Variables
--------
The drill integration allows you to start your notebook with certain ENV variables for ease of connection to Drill clusters
- JUPYTER_DRILL_USER
  - Username to connect to Drill with. This helps the user experience in that the user isn't prompted for a user. 
- JUPYTER_DRILL_BASE_URL
  - The connection URL for your Drill cluster
  - This is ignored (and replaced with http://localhost:8047) in embedded mode
  - Format is scheme://hostorIP:port i.e. https://drillcluster.mycorp:8047

