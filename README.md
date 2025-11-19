# **How to Build the RSBSA Toolbelt as a Standalone App**

## **⚠️ Crucial Note for Mac Users**

PyInstaller builds for the OS you are currently using.

* **Running on Mac?** You will get a Mac executable.  
* **Running on Windows?** You will get a Windows .exe.

**You cannot build a Windows .exe directly on a Mac.**

If you are on a Mac but need a Windows app, use **Option C** below.

## **Option A: Optimized Single File (Windows Native)**

*Run this ON A WINDOWS COMPUTER to get a .exe*

pyinstaller \--onefile \--name="RSBSAToolbelt\_Optimized" \--exclude-module matplotlib \--exclude-module tkinter \--exclude-module scipy \--exclude-module PIL rsbsa\_toolbelt.py

* **Result:** A single .exe in dist/.  
* **Startup:** \~3-5 seconds.

## **Option B: Folder (Instant Start)**

*Run this ON A WINDOWS COMPUTER to get a .exe*

pyinstaller \--onedir \--name="RSBSAToolbelt\_Fast" rsbsa\_toolbelt.py

* **Result:** A folder in dist/.  
* **Startup:** Instant.

## **Option C: Building for Windows using GitHub (For Mac Users)**

If you don't have a Windows PC handy, use GitHub Actions:

1. Create a folder .github/workflows in your project.  
2. Add the build\_windows.yml file provided.  
3. Push your code to GitHub.  
4. Go to the **Actions** tab on your repository page.  
5. Click on the latest workflow run.  
6. Scroll down to **Artifacts** and download your .exe.