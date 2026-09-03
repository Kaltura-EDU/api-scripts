# How to setup PyCharm to use these scripts
This guide assumes that you have pycharm already installed, if not get it @ <a href="https://www.jetbrains.com/pycharm/download/?section=windows" target="_blank" rel="noopener noreferrer">PyCharm Downloads</a>

## Initial Steps:
1. Download the scripts you want to use, requirements.txt, and the blank.env  
2. Make a copy of the blank.env file, and using a text editor, enter your institutions information, and your email address, saving as "project.env" or something similar  

## Setup PyCharm
1. Launch PyCharm  
2. File > Create a new project  
3. Select **Project venv**  
4. Click **Create**
5. When the notification about _Microsoft Defender_ pops up, chose **Exclude Folders**  
6. In the Project view, expand the _.venv_, and _Scripts_ folders  
7. Copy the .py script, requirements.txt, to the _Scripts_ folder  
8. On the left, click on Terminal (also Alt-F12)
9. In the terminal pane, type the following:
   > cd .venv\Scripts  
   > python activate_this.py  
   > cp project.env .env  
   > pip install -r requirements.txt  
   > python _scriptname.py_



