Hey all! 

In this section of the site you'll find Python scripts that use Kaltura's API to perform some tasks. In order to run these scripts, you'll have use a command line interface (e.g. "Terminal" on a Mac). There are a few additional steps I'll articulate in more detail below.

In general, I recommend that you keep all of your scripts in the same folder on your computer. 

It's also recommended that you create a virtual environment, the data for which will be stored in the folder where you store all of the scripts. You'll only need to do this once. 


# HOW TO CREATE A VIRTUAL ENVIRONMENT

1. Open Terminal (Mac) or Command Prompt (Windows) and navigate to the folder in question (e.g. cd /path/to/project).
2. Type `python3 -m venv venv` Note that this assumes you’re running the latest version of Python. If not, just use “python” instead of “python3”.

So why should you do this? When you run a Python script, sometimes you have to install "modules" on your computer that allow you to do certain types of things. For example, for all of the scripts on this site, you'll almost certainly have to have "KalturaApiClient" installed so Python knows what the heck you're talking about when you say "baseEntry.get." It's good to install these modules within your virtual environment because there's a risk that certain functions or API actions might have the same name in multiple modules (but do different things). Having a virtual environment isolates the scripts' dependencies. 

Once you've created your virtual environment. You'll need to activate it. You'll need to do this every time you want to run the script. 


# HOW TO ACTIVATE A VIRTUAL ENVIRONMENT 

1. Open Terminal (Mac) or Command Prompt (Windows) and navigate to the folder in question (e.g. `cd /path/to/project`).
2. Now actiavate the virtual environment:
  - Windows: `venv\\Scripts\\activate`
  - Mac: `source venv/bin/activate`


# HOW TO INSTALL MODULES

Once you've activated the virtual environment, you'll need to install the modules that the script needs in order to run. 

Note that once you start using pip to install modules, you may be prompted to upgrade it, which I'd recommend. The modules you'll need are contained in the Requirements.txt file. So before you run any script, you'll need to install the modules within your virtual environment. Download the Requirements.txt file to the directory where you store your scripts. Assuming you've already activated your virtual environment, type the following:

    pip install -r Requirements.txt

You may need to periodically install new modules as needed. 


# HOW TO FIND YOUR ADMIN SECRET

Some scripts on this site may require you to find your *admin secret*. It's a private key used to authenticate API requests with administrative privileges. It allows a script or application to perform actions on behalf of an account, such as managing media, users, or settings. Admin secrets should always be kept confidential to prevent unauthorized access. Here's how to find yours:

1. Go to https://developer.kaltura.com.
2. Click the **Sign in** button at the top of the page.
3. Enter the same credentials you use for the KMC.
4. If applicable, select the approprite partner ID (PID). 
5. In the upper right corner of the page, click the pull down menu that shows your client name and PID and select **View Secrets**.

-Galen Davis, Senior Ed Tech Specialist, UC San Diego
27 January 2025
