Hey all! 

In this section of the site you'll find Python scripts that use Kaltura's API to perform some tasks. In order to run these scripts, you'll have use a command line interface (e.g. "Terminal" on a Mac). There are a few additional steps I'll articulate in more detail below.

In general, I recommend that you keep all of your scripts in the same folder on your computer. 

It's also recommended that you create a virtual environment, the data for which will be stored in the folder where you store all of the scripts. You'll only need to do this once. 

-----------------------------------
HOW TO CREATE A VIRTUAL ENVIRONMENT (one-time instructions)
-----------------------------------

  1. Open Terminal (Mac) or Command Prompt (Windows) and navigate to the folder in question (e.g. cd /path/to/project).
  2. Type 

python3 -m venv venv
     
     Note that this assumes you’re running the latest version of Python. If not, just use “python” instead of “python3”.

So why should you do this? When you run a Python script, sometimes you have to install "modules" on your computer that allow you to do certain types of things. For example, for all of the scripts on this site, you'll almost certainly have to have "KalturaApiClient" installed. It's good to install these modules within your virtual environment because there's a risk that certain functions or API actions might have the same name in multiple modules (but do different things). Having a virtual environment isolates the scripts' dependencies. 

Once you've created your virtual environment. You'll need to activate it. You'll need to do this every time you want to run the script. 

-------------------------------------
HOW TO ACTIVATE A VIRTUAL ENVIRONMENT (do this every time you want to run scripts)
-------------------------------------

  1. Open Terminal (Mac) or Command Prompt (Windows) and navigate to the folder in question (e.g. cd /path/to/project).
  2. Now actiavate the virtual environment:
       Windows: venv\\Scripts\\activate
       Mac: source venv/bin/activate

Once you've activated the virtual environment, you'll need to install the modules that the script needs in order to run. 

-- Galen Davis, Senior Ed Tech Specialist, UC San Diego
27 January 2025
