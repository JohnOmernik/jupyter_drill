# jupyter_drill
A module to help interaction with Jupyter Notebooks and Apache Drill

------
This is a python module that helps to connect Jupyter Notebooks to various datasets. 
It's based on (and requires) https://github.com/JohnOmernik/jupyter_integration_base 


## Initialization 
----
After installing this, to instantiate the module so you can use %drill and %%drill put this in a cell:

the Drill() init can take the following arguments, or they can be left off to use these defaults

debug=False, pd_display_grid="html", drill_conn_url_default=""

- debug
  - Turns on addition logging
- pd_display_grid
  - html - Use standard html display in data frames (default)
  - qgrid - Use qgrid (https://github.com/quantopian/qgrid v. 1.3 or higher) Recommended 
- drill_conn_url_default
  - Can set the default connection url at instantiation (Can only set the one instance, use ENV variables for multiple instances)



### Example Inits

#### Debug false, using qgrid, no connection URL specified
```
ipy = get_ipython()

from drill_core import Drill
Drill = Drill(ipy, debug=False, pd_display_grid="qgrid")
ipy.register_magics(Drill)
```

#### Debug false, using html display, connection url specified for embedded mode:

```
ipy = get_ipython()

from drill_core import Drill
Drill = Drill(ipy, debug=False, pd_display_grid="qgrid", drill_conn_url_default="http://drill@localhost:8047?drill_embedded=1")
ipy.register_magics(Drill)
```


http://drill@localhost:8047?drill_embedded=1


#### Debug false, use html, and read from ENV (use all instantiation defaults)

Explicit

```
from drill_core import Drill
ipy = get_ipython()
Drill = Drill(ipy, debug="", pd_display_grid="html", drill_conn_url_default="")
ipy.register_magics(Drill)
```

Implicit

```
from drill_core import Drill
ipy = get_ipython()
Drill = Drill(ipy)
ipy.register_magics(Drill)
```

## Instance Usage
--------
You can use multiple instances of Drill with the same magic function.  You do this by specifying instances in the ENV variables (see ENV Variables below).  Multiple functions can use the instances, connect, disconnect and queries. 

In addition, you can look and see instance information by typing %drill instances

One more thing: If you are connecting to an instance that requires a password, and you have ALREADY set a password on the default instance, it WILL attempt to connect with the default instance password.  In many installations, the password is the same. 

If you wish to set a different password for a non-default instance, you have two options:

### Option one connect alt
```
%drill connect myinstance alt
```
This will ask for the connection URL, and password for an instance. If you dont' want to type the connection url every time, try:


### Option two setpass
```
%drill setpass myinstance
```
This will set the password for my instance, but DOES NOT connect to it. After you have set the password then type

```
%drill connect myinstance
```
And this will use the instance set password. 


Normally, if you provide a magic line function like

```
%drill connect
```

It will automatically default to the instance specified in the JUPYTER_DRILL_CONN_DEFAULT Env Variable. If you want to use a specific instance, you can use:

```
%drill connect myinstance
```
And it will utilize that instance. 

In addition for queries, the same applies, if you do not specify an instance name, the query will be submitted to the JUPYTER_DRILL_CONN_DEFAULT Env Variable value

```
%%drill
select * from sys.options
```

However, if you wish to specify an instance name you can use:

```
%%drill myinstance
select * from sys.options
```

## ENV Variables
--------
To allow multiple instances, drill lets you specify two main ENV variables:
  - JUPTYER_DRILL_CONN_URL_X - The connection URL for the instance x
    - Note in the ENV variable, you ucase the instance name (X) in the JUPYTER_DRILL_CONN_URL_ variable
    - However, it is referenced both in the magics and in the default ENV as lcase (x)
  - JUPYTER_DRILL_CONN_DEFAULT - This (not the lack of URL) is the DEFAULT connection instance that Drill will use. 


### URL Format
```
scheme://user@host:port?option1=option1val&option2=option2val&option3=option3val
```

For drill here are the items to consider:
- scheme
  - This will be http or https for Drill instances
- user
  - This is the username to connect with.
  - Note in embedded mode, this is ignored, I just put in drill as the user for embedded mode
- host
  - Hostname to connect with.
  - Default (and embedded mode host) should be localhost
- port
  - Port to connect with
  - Default (and embedded mode) is 8047
- Options
  - after the ? options are k=v pairs sep by &
  - One big one is drill_embedded=1  This sets embedded mode and doesn't require a password


### Example ENVs when starting Jupyter Lab
----
```
export JUPYTER_DRILL_CONN_URL_DEFAULT="http://drill@localhost:8047?drill_embedded=1"
export JUPYTER_DRILL_CONN_DEFAULT="default"
```

This creates a default instance, uses embedded mode, and sets the default instance to the instance name of "default"

