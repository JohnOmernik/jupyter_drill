#!/usr/bin/python

# Base imports for all integrations, only remove these at your own risk!
import json
import sys
import os
import time
import pandas as pd
from collections import OrderedDict

from integration_core import Integration

from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from IPython.core.display import HTML

# Your Specific integration imports go here, make sure they are in requirements!
import requests
import socket
from requests.packages.urllib3.exceptions import SubjectAltNameWarning, InsecureRequestWarning
#from requests_toolbelt.adapters import host_header_ssl
requests.packages.urllib3.disable_warnings(SubjectAltNameWarning)

#import IPython.display
from IPython.display import display_html, display, Javascript, FileLink, FileLinks, Image
import ipywidgets as widgets

@magics_class
class Drill(Integration):
    # Static Variables
    # The name of the integration
    name_str = "drill"
    custom_evars = ['drill_conn_default']
    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    custom_allowed_set_opts = ['drill_max_rows', 'drill_conn_default', 'drill_verify', 'drill_ignore_ssl_warn', 'drill_verbose_errors', 'drill_embedded'] 

    myopts = {} 
    myopts['drill_max_rows'] = [1000, 'Max number of rows to return, will potentially add this to queries']
    myopts['drill_conn_default'] = ['default', 'Default instance name to use for connections']
 
    myopts['drill_headers'] = [{}, "Custom Headers to use for Drill connections"]
    myopts['drill_verify'] = ['/etc/ssl/certs/ca-certificates.crt', "Either the path to the CA Cert validation bundle or False for don't verify"]
    myopts['drill_ignore_ssl_warn'] = [0, "Supress SSL warning upon connection - Not recommended"]
    myopts['drill_verbose_errors'] = [0, "Show more verbose errors if available"]
    myopts['drill_embedded'] = [0, "Connect without username/password and without sessions"]

#    myopts[name_str + '_user'] = ["drill", "User to connect with  - Can be set via ENV Var: JUPYTER_" + name_str.upper() + "_USER otherwise will prompt"]
#    myopts[name_str + '_base_url_host'] = ["", "Hostname of connection derived from base_url"]
#    myopts[name_str + '_base_url_port'] = ["", "Port of connection derived from base_url"]
#    myopts[name_str + '_base_url_scheme'] = ["", "Scheme of connection derived from base_url"]



    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, pd_display_grid="html", drill_conn_url_default="", debug=False, *args, **kwargs):
        super(Drill, self).__init__(shell, debug=debug, pd_display_grid=pd_display_grid)
        self.debug = debug

        self.opts['pd_display_grid'][0] = pd_display_grid
        if pd_display_grid == "qgrid":
            try:
                import qgrid
            except:
                print ("WARNING - QGRID SUPPORT FAILED - defaulting to html")
                self.opts['pd_display_grid'][0] = "html"

        #Add local variables to opts dict
        for k in self.myopts.keys():
            self.opts[k] = self.myopts[k]

        self.load_env(self.custom_evars)
        if drill_conn_url_default != "":
            if "default" in self.instances.keys():
                print("Warning: default instance in ENV and passed to class creation - overwriting ENV")
            self.fill_instance("default", drill_conn_url_default)
                
        self.parse_instances()


    def customAuth(self, instance):
        result = -1
        inst = None

        if instance not in self.instances.keys():
            print("Instance %s not found in instances - Connection Failed" % instance)
            result = -3
        else:
            inst = self.instances[instance]            

        if inst is not None:
            inst['session'] = None
            inst['session'] = requests.Session()
            inst['session'].allow_redirects = False
            inst['drill_url'] = inst['scheme'] + "://" + inst['host'] + ":" + str(inst['port'])

            if self.checkvar(instance, 'drill_ignore_ssl_warn') == 1:
                print("Warning: Setting session to ignore SSL warnings - Use at your own risk")
                requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


            if self.checkvar(instance, 'drill_embedded') == 1:
                print("Drill Embedded Selected, session variables will not work!")
                inst['connected'] = True
                result = 0
            else:
                # Get the connect URL
                myurl = inst['drill_url'] + "/j_security_check"
                if self.debug:
                    print("")
                    print("Connecting URL: %s" % myurl)
                    print("")
    
                # Create the user/pass string
                mypass = ""
                if inst['connect_pass'] is None:
                    mypass = self.instances[self.opts[self.name_str + "_conn_default"][0]]['connect_pass']
                else:
                    mypass = inst['connect_pass']

                login = {'j_username': inst['user'], 'j_password': mypass}
 
                if self.debug:
                    print("")
                    print("Headers in auth: %s" % self.checkvar(instance, 'drill_headers'))
                    print("")
                    print("Adapters: %s" % self.session.adapters)

            # Make the connect attempt
                r = inst["session"].post(myurl, allow_redirects=self.session.allow_redirects, data=login, headers=self.checkvar(instance, 'drill_headers'), verify=self.checkvar(instance, 'drill_verify'))

            # Parse the response
                if r.status_code == 200:
                    if r.text.find("Invalid username/password credentials") >= 0: # This is an incorrect login
                        result = -2
                        raise Exception("Invalid username/password credentials")
                    elif r.text.find('<li><a href="/logout">Log Out (') >= 0: # This is a success
                        pass
                        result = 0
                    else:
                        result = -1
                        raise Exception("Unknown HTTP 200 Code: %s" % r.text)
                elif r.status_code == 303: # This may be success too
                    pass
                    result = 0
                else: # Unknown error return -1 
                    raise Exception("Status Code: %s - Error" % r.status_code)
                    result = -1

        return result



    def validateQuery(self, query, instance):


        bRun = True
        bReRun = False
        if self.instances[instance]['last_query'] == query:
            # If the validation allows rerun, that we are here:
            bReRun = True
        # Ok, we know if we are rerun or not, so let's now set the last_query (and last use if needed) 
        self.instances[instance]['last_query'] = query
        if query.strip().find("use ") == 0:
            self.instances[instance]['last_use'] = query


        # Example Validation

        # Warn only - Don't change bRun
        # This one is looking for a ; in the query. We let it run, but we warn the user
        # Basically, we print a warning but don't change the bRun variable and the bReRun doesn't matter
        if query.find(";") >= 0:
            print("WARNING - Do not type a trailing semi colon on queries, your query will fail (like it probably did here)")

        # Warn and don't submit after first attempt - Second attempt go ahead and run
        # If the query doesn't have a day query, then maybe we want to WARN the user and not run the query.
        # However, if this is the second time in a row that the user has submitted the query, then they must want to run without day
        # So if bReRun is True, we allow bRun to stay true. This ensures the user to submit after warnings
        if query.lower().find("limit ") < 0:
            print("WARNING - Queries shoud have a limit so you don't bonkers your DOM")
        # Warn and do not allow submission
        # There is no way for a user to submit this query 
#        if query.lower().find('limit ") < 0:
#            print("ERROR - All queries must have a limit clause - Query will not submit without out")
#            bRun = False
        return bRun

    def customQuery(self, query, instance):
        url = self.instances[instance]['drill_url'] + "/query.json"
        payload = {"queryType":"SQL", "query":query}
        cur_headers = self.checkvar(instance, 'drill_headers')
        cur_headers["Content-type"] = "application/json"
        mydf = None
        status = ""
        try:
            r = self.instances[instance]['session'].post(url, data=json.dumps(payload), headers=cur_headers, verify=self.checkvar(instance, 'drill_verify'))
            if r.status_code == 200:
                if r.text.find("Invalid username/password credentials.") >= 0:
                    print("It looks like your Drill Session has expired, please run %drill connect to resolve")
                    self.disconnect(instance)
                    self.ipy.set_next_input("%drill connect " + instance)
                    status = "Failure: Session Expired"
                else:
                    try:
                        jrecs = json.loads(r.text, object_pairs_hook=OrderedDict)
                        try:
                            cols = jrecs['columns']
                            rows = jrecs['rows']
                            if len(cols) == 0 or len(rows) == 0:
                                status = "Success - No Results"
                                mydf = None
                            else:
                                status = "Success"
                                mydf = pd.read_json(json.dumps(rows))
                                mydf = mydf[cols]
                        except:
                            if len(cols) == 0 or len(rows) == 0:
                                status = "Success - No Results"
                                mydf = None
                    except:
                        status = "Failure: Error Loading JSON records or parsing into dataframe"
            else:
                if self.debug:
                    print("Oops error: Code: %s - Text: %s" % (r.status_code, r.text))
                status = "Failure: Non - 200 Error - %s - %s" % (r.status_code, r.text)
                mydf = None
        except Exception as e:
            mydf = None
            str_err = str(e)
            if self.debug:
                print("Error: %s" % str(e))
            if self.checkvar(instance, 'drill_verbose_errors') == True:
                status = "Failure - query_error: " + str_err
            else:
                msg_find = "errorMessage=\""
                em_start = str_err.find(msg_find)
                find_len = len(msg_find)
                em_end = str_err[em_start + find_len:].find("\"")
                str_out = str_err[em_start + find_len:em_start + em_end + find_len]
                status = "Failure - query_error: " + str_out
        return mydf, status

# Display Help can be customized
    def customHelp(self):
        self.displayIntegrationHelp()
        self.displayQueryHelp("select * from dfs.`mydatabase`.`mytable`")


    # This is the magic name.
    @line_cell_magic
    def drill(self, line, cell=None):
        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)
            if self.debug:
                print("line: %s" % line)
                print("cell: %s" % cell)
            if not line_handled: # We based on this we can do custom things for integrations. 
                if line.lower() == "testintwin":
                    print("You've found the custom testint winning line magic!")
                else:
                    print("I am sorry, I don't know what you want to do with your line magic, try just %" + self.name_str + " for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)

