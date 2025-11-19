# **How to Build a Standalone .exe**

This guide will turn your Python script into a single .exe file that you can email to colleagues or put on a shared drive. They do not need Python installed to run it.

## **1\. Preparation**

Ensure you have the necessary libraries installed:

pip install \-r requirements.txt

## **2\. The Build Command**

Run the following command in your terminal.

*Note: We use excel\_merger.py (the GUI version), not the web version.*

pyinstaller \--noconsole \--onefile \--name="ExcelMerger" excel\_merger.py

### **What do these flags do?**

* \--noconsole: The app will open like a normal Windows app, without a black command prompt window appearing behind it.  
* \--onefile: Bundles everything (Python, Pandas, your code) into a single .exe file.  
* \--name: Sets the name of your output file.

## **3\. Locate your App**

Once the command finishes (it may take 1-2 minutes):

1. Go to the new **dist** folder created in your project directory.  
2. You will find **ExcelMerger.exe**.

## **4\. Testing & Sharing**

* Move ExcelMerger.exe to a different folder or computer to test it.  
* You can now zip this file and share it\!

## **Common Questions**

Q: Why is the file size large (\~50MB+)?  
A: It contains the entire Python engine and the Pandas library embedded inside it so your users don't need to install anything.  
Q: Can I build a Mac app on Windows?  
A: No. PyInstaller builds for the OS you are currently using. To build a Mac .app file, you must run these commands on a Mac.