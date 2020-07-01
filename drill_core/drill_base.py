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
    custom_evars = [name_str + "_base_url", name_str + "_user"]
    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    custom_allowed_set_opts = [name_str + '_base_url', name_str + '_verify', name_str + '_pin_to_ip', name_str + '_rewrite_host', name_str + '_ignore_ssl_warn', name_str + '_inc_port_in_rewrite', name_str + '_embedded', name_str + '_verbose_errors'] 

    
    myopts = {} 
    myopts[name_str + '_max_rows'] = [1000, 'Max number of rows to return, will potentially add this to queries']
    myopts[name_str + '_user'] = ["drill", "User to connect with  - Can be set via ENV Var: JUPYTER_" + name_str.upper() + "_USER otherwise will prompt"]
    myopts[name_str + '_base_url'] = ["http://localhost:8047", "URL to connect to server. Can be set via ENV Var: JUPYTER_" + name_str.upper() + "_BASE_URL"]
    myopts[name_str + '_base_url_host'] = ["", "Hostname of connection derived from base_url"]
    myopts[name_str + '_base_url_port'] = ["", "Port of connection derived from base_url"]
    myopts[name_str + '_base_url_scheme'] = ["", "Scheme of connection derived from base_url"]

    myopts[name_str + '_verbose_errors'] = [False, "Show more verbose errors if available"]
    myopts[name_str + '_embedded'] = [False, "Connect without username/password and without sessions"]
    myopts[name_str + '_pin_to_ip'] = [False, "Obtain an IP from the name and connect directly to that IP"]
    myopts[name_str + '_pinned_ip'] = ["", "IP of pinned connection"]
    myopts[name_str + '_rewrite_host'] = [False, "When using Pin to IP, rewrite the host header to match the name of base_url"]
    myopts[name_str + '_inc_port_in_rewrite'] = [False, "When rewriting the host header, include :%port% in the host header"]
    myopts[name_str + '_headers'] = [{}, "Customer Headers to use for Drill connections"]
    myopts[name_str + '_url'] = ['', "Actual URL used for connection (base URL is the URL that is passed in as default"]
    myopts[name_str + '_verify'] = ['/etc/ssl/certs/ca-certificates.crt', "Either the path to the CA Cert validation bundle or False for don't verify"]
    myopts[name_str + '_ignore_ssl_warn'] = [False, "Supress SSL warning upon connection - Not recommended"]
    myopts[name_str + '_last_query'] = ["", "The last query attempted to be run"]
    myopts[name_str + '_last_use'] = ["", "The use (database) statement ran"]

    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, pd_display_grid="html", drill_rewrite_host=False, drill_pin_to_ip=False, drill_embedded=False, *args, **kwargs):
        super(Drill, self).__init__(shell)
        self.load_env(self.custom_evars)
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

        self.opts['drill_embedded'][0] = drill_embedded
        self.opts['drill_pin_to_ip'][0] = drill_pin_to_ip
        self.opts['drill_rewrite_host'][0] = drill_rewrite_host
        self.opts['pd_display_grid'][0] = pd_display_grid


    def disconnect(self):
        if self.connected == True:
            print("Disconnected %s Session from %s" % (self.name_str.capitalize(), self.opts[self.name_str + '_base_url'][0]))
        else:
            print("%s Not Currently Connected - Resetting All Variables" % self.name_str.capitalize())
        self.mysession = None
        self.connect_pass = None
        self.connected = False


    def connect(self, prompt=False):
        if self.connected == False and self.opts['drill_embedded'][0] == False:
            if prompt == True or self.opts[self.name_str + '_user'][0] == '':
                print("User not specified in %s%s_USER or user override requested" % (self.env_pre, self.name_str.upper()))
                tuser = input("Please type user name if desired: ")
                self.opts[self.name_str + '_user'][0] = tuser
            print("Connecting as user %s" % self.opts[self.name_str + '_user'][0])
            print("")

            if prompt == True or self.opts[self.name_str  + "_base_url"][0] == '':
                print("%s Base URL not specified in %s%s_BASE_URL or override requested" % (self.env_pre, self.name_str.capitalize(), self.name_str.upper()))
                turl = input("Please type in the full %s URL: " % self.name_str.capitalize())
                self.opts[self.name_str + '_base_url'][0] = turl
            print("Connecting to %s URL: %s" % (self.name_str.capitalize(), self.opts['_base_url'][0]))
            print("")

            myurl = self.opts[self.name_str + '_base_url'][0]
            ts1 = myurl.split("://")
            self.opts[self.name_str + '_base_url_scheme'][0] = ts1[0]
            t1 = ts1[1]
            ts2 = t1.split(":")
            self.opts[self.name_str + '_base_url_host'][0] = ts2[0]
            self.opts[self.name_str + '_base_url_port'][0] = ts2[1]

#            Use the following if your data source requries a password
            print("Please enter the password you wish to connect with:")
            tpass = ""
            self.ipy.ex("from getpass import getpass\ntpass = getpass(prompt='Connection Password: ')")
            tpass = self.ipy.user_ns['tpass']

            self.connect_pass = tpass
            self.ipy.user_ns['tpass'] = ""

            if self.opts['drill_ignore_ssl_warn'][0] == True:
                print("Warning: Setting session to ignore SSL warnings - Use at your own risk")
                requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

            result = self.auth()

            if result == 0:
                self.connected = True
                print("%s - %s Connected!" % (self.name_str.capitalize(), self.opts[self.name_str + '_base_url'][0]))
            else:
                print("Connection Error - Perhaps Bad Usename/Password?")

        elif self.connected == True and self.opts['drill_embedded'][0] == False:
            print(self.name_str.capitalize() + "is already connected - Please type %" + self.name_str + " for help on what you can you do")
        elif self.opts['drill_embedded'][0] == True:
            self.session = requests.Session()
            print("Drill Embedded Selected, sessions will not work!")
            print("Using http://localhost:8047 as url for embedded mode")
            myurl = "http://localhost:8047"
            ts1 = myurl.split("://")
            self.opts[self.name_str + '_base_url_scheme'][0] = ts1[0]
            t1 = ts1[1]
            ts2 = t1.split(":")
            self.opts[self.name_str + '_base_url_host'][0] = ts2[0]
            self.opts[self.name_str + '_base_url_port'][0] = ts2[1]
            self.opts[self.name_str + '_base_url'][0] = myurl
            self.opts[self.name_str + '_url'][0] = myurl
            self.connected = True

        if self.connected != True:
            self.disconnect()

    def auth(self):
        self.session = None
        result = -1
        self.session = requests.Session()
        self.session.allow_redirects = False
        # Handle weird pin_to_ip situations
        if self.opts[self.name_str + '_pin_to_ip'][0] == True:
                self.opts['drill_pinned_ip'][0] = self.getipurl(self.opts['drill_base_url'][0])
                print("")
                print("Pinning to IP for this session: %s" % self.opts['drill_pinned_ip'][0])
                print("")
                self.opts['drill_url'][0] = "%s://%s:%s" % ( self.opts['drill_base_url_scheme'][0],  self.opts['drill_pinned_ip'][0] ,  self.opts['drill_base_url_port'][0])
                if self.opts['drill_rewrite_host'][0] == True:
                    #self.session.mount("https://", host_header_ssl.HostHeaderSSLAdapter())
                    if self.opts['drill_inc_port_in_rewrite'][0] == True:
                        self.opts['drill_headers'][0]['host'] = self.opts['drill_base_url_host'][0] + ":" + self.opts['drill_base_url_port'][0]
                    else:
                        self.opts['drill_headers'][0]['host'] = self.opts['drill_base_url_host'][0]
                    if self.debug:
                        print("Headers in connect: %s" % self.opts['drill_headers'][0])
        else:
            self.opts['drill_url'][0] = self.opts['drill_base_url'][0]

        # Get the connect URL
        myurl = self.opts['drill_url'][0] + "/j_security_check"
        if self.debug:
            print("")
            print("Connecting URL: %s" % myurl)
            print("")
    
        # Create the user/pass string
        login = {'j_username': self.opts['drill_user'][0], 'j_password': self.connect_pass}
 
        if self.debug:
            print("")
            print("Headers in auth: %s" % self.opts['drill_headers'][0])
            print("")
        if self.debug:
            print("Adapters: %s" % self.session.adapters)

        # Make the connect attempt
        r = self.session.post(myurl, allow_redirects=self.session.allow_redirects, data=login, headers=self.opts['drill_headers'][0], verify=self.opts['drill_verify'][0])


        # Parse the response
        if r.status_code == 200:
            if r.text.find("Invalid username/password credentials") >= 0: # This is an incorrect login
                result = -2
                raise Exception("Invalid username/password credentials")
            elif r.text.find('<li><a href="/logout">Log Out (') >= 0: # This is a success
                pass
                result = 0
            else:
                raise Exception("Unknown HTTP 200 Code: %s" % r.text)
        elif r.status_code == 303: # This may be success too
            pass
            result = 0
        else: # Unknown error return -1 
            raise Exception("Status Code: %s - Error" % r.status_code)


        return result


    def validateQuery(self, query):
        bRun = True
        bReRun = False
        if self.opts[self.name_str + "_last_query"][0] == query:
            # If the validation allows rerun, that we are here:
            bReRun = True
        # Ok, we know if we are rerun or not, so let's now set the last_query 
        self.opts[self.name_str + "_last_query"][0] = query


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

    def customQuery(self, query):
        url = self.opts['drill_url'][0] + "/query.json"
        payload = {"queryType":"SQL", "query":query}
        cur_headers = self.opts['drill_headers'][0]
        cur_headers["Content-type"] = "application/json"
        mydf = None
        status = ""
        try:
            r = self.session.post(url, data=json.dumps(payload), headers=cur_headers, verify=self.opts['drill_verify'][0])
            if r.status_code == 200:
                if r.text.find("Invalid username/password credentials.") >= 0:
                    print("It looks like your Drill Session has expired, please run %drill connect to resolve")
                    self.disconnect()
                    self.ipy.set_next_input("%drill connect")
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
            if self.opts['drill_verbose_errors'][0] == True:
                status = "Failure - query_error: " + str_err
            else:
                msg_find = "errorMessage=\""
                em_start = str_err.find(msg_find)
                find_len = len(msg_find)
                em_end = str_err[em_start + find_len:].find("\"")
                str_out = str_err[em_start + find_len:em_start + em_end + find_len]
                status = "Failure - query_error: " + str_out
        return mydf, status




# Display Help must be completely customized, please look at this Hive example
    def customHelp(self):
        print("jupyter_drill is a interface that allows you to use the magic function %drill to interact with an Drill installation.")
        print("")
        print("jupyter_drill has two main modes %drill and %%drill")
        print("%drill is for interacting with a Drill installation, connecting, disconnecting, seeing status, etc")
        print("%%drill is for running queries and obtaining results back from the Drill cluster")
        print("")
        print("%drill functions available")
        print("###############################################################################################")
        print("")
        print("{: <30} {: <80}".format(*["%drill", "This help screen"]))
        print("{: <30} {: <80}".format(*["%dtill status", "Print the status of the Hive connection and variables used for output"]))
        print("{: <30} {: <80}".format(*["%drill connect", "Initiate a connection to the Hive cluster, attempting to use the ENV variables for Hive URL and Hive Username"]))
        print("{: <30} {: <80}".format(*["%drill connect alt", "Initiate a connection to the Hive cluster, but prompt for Username and URL regardless of ENV variables"]))
        print("{: <30} {: <80}".format(*["%drill disconnect", "Disconnect an active Hive connection and reset connection variables"]))
        print("{: <30} {: <80}".format(*["%drill set %variable% %value%", "Set the variable %variable% to the value %value%"]))
        print("{: <30} {: <80}".format(*["%drill debug", "Sets an internal debug variable to True (False by default) to see more verbose info about connections"]))
        print("")
        print("Running queries with %%drill")
        print("###############################################################################################")
        print("")
        print("When running queries with %%drill, %%drill will be on the first line of your cell, and the next line is the query you wish to run. Example:")
        print("")
        print("%%drill")
        print("select * from dfs.`mydatabase`.`mytable`")
        print("")
        print("Some query notes:")
        print("- If the number of results is less than pd_display.max_rows, then the results will be diplayed in your notebook")
        print("- You can change pd_display.max_rows with %drill set pd_display.max_rows 2000")
        print("- The results, regardless of display will be place in a Pandas Dataframe variable called prev_drill")
        print("- prev_drill is overwritten every time a successful query is run. If you want to save results assign it to a new variable")




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
                    print("I am sorry, I don't know what you want to do with your line magic, try just %" + self.name_str + "for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell)

