# Description
This script deletes certain types of so-called "cuePoints" from entries based on a comma-delimited list of entry IDs: chapters, quiz questions, or quiz answers (aka user submissions). 

According to Kaltura in their cuePoint.py plugin file, there are several types of cuePoints:

- AD = "adCuePoint.Ad"
- ANNOTATION = "annotation.Annotation"
- CODE = "codeCuePoint.Code"
- EVENT = "eventCuePoint.Event"
- QUIZ_ANSWER = "quiz.QUIZ_ANSWER"
- QUIZ_QUESTION = "quiz.QUIZ_QUESTION"
- THUMB = "thumbCuePoint.Thumb"

This focuses only on thumbCuePoint.Thumb (chapter markers), quiz.QUIZ_ANSWER (user submissions to a quiz), and quiz.QUIZ_QUESTION (the questions inserted into a Kaltura video quiz).

# Instructions
1. Download **delete-cue-points.py** and **Requirements.txt** to your computer.
2. Open **rename-entries.py** with a text editor.
3. Open a command line interface, such as Terminal on a Mac or Command Prompt in Windows.
4. Navigate to wherever you put your files (e.g. `cd /path/to/project`).
5. Set up a virtual environment if you haven't already: `python3 -m venv venv`
6. Install the needed modules: `pip install -r requirements.txt`
7. Activate your virtual environment (Windows: `venv\\Scripts\\activate` Mac: `source venv/bin/activate`)
8. Run the script: `python3 delete-cue-points.py`

---

Galen Davis  
Senior Education Technology Specialist  
UC San Diego  

*and* 

Andy Clark  
Systems Administrator, Learning Systems  
Baylor University  

*Last updated 2025-05-05*