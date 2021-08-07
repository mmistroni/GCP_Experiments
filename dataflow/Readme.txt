==========================
This directory contains various dataflow jobs which were originally ran using standard dataflow templates
The following subdirectories contains dataflow jobs which have been moved to use flex templates

- edgar_flow
- pipeline
- shareloader

There's a run_flow_template.txt   file that lists all the commans i ran to kick off the flex template

==== EDGAR FLOW ====
This small subproject contains a job which , in its simplicity, does the following
1. accepts a Quarter and a year parameter
2. resolves an URL for downloading Edgar informations (sec.gov)
3. does some transformations
4. store data in a bucket

For this project to run you might need to have permissions to access APIs like sendmail / IEXAPI / , however you should be able to customize the code as you please
This job is fairly complex in the sense that the workload is splitted across different modules in subdirectories
The code had to be rearranged (especially imports,   something like  from  .localmodule will not work in flex), but it is now running fine

====PIPELINE ===
This subproject is very similar to edgar_flow in the sense that , when attempting to use flex templates, i started off  by following GCP flex samples and then
customized the code so that it can read a file remotely and it can write it to a GCP bucket


==== TROUBLESHOOTING FLEX TEMPLATES ============
I came across multiple issues while attempting to run my jobs using flex templates.... which i outline below
1. LOCAL IMPORTS.   Local imports such as   from .moduleinsamedirectory import abc  will not work. Instead, if you have submodules which you want to refer 
   in some other files, you will always have to refer them like, eg   from modules.utility import returnSomething  (check pipeline.modules.edgar_quarterly_form4.py
   )
2. SUBMODULES.  If your code is splitted across different modules in subdirectories, you will need a setup.py to make Dataflow understand it
3. PERMISSIONS. When running all different commands for creating a flex template, i do so in Cloud SHell. Apparently you need to have certain permissions 
   to execute all these commands and create images etc.. Running everything from Cloud Shell console is straightforward as you'll already have most of the permissions  
   you need
4. SETUP.PY: if a setup.py is n eeded, you will need to pass  the file as input parameter of the job (- see edgar_flow\run_flow_templates.txt)
5. DOCKERFILE.  Make sure your dockerfile copies all the right files .  I sampled mine from GCP flex samples. YOu'll see that pipeline\Dockerfile is very similar 
   to edgar_flow\Dockerfile.  Pay attention when you reuse the file for other projects... make sure all the files are copied properly. In your dockerfile, try
   to run LS commands to see what your directory looks like after the Docker file.  More than once i discovered i forgot to copy files, or i referenced 
   files that ddint exist. This will result in awful errors like your job not starting and no logs available to figure out what is going 
6. LOGS. If your job kicks off , whether it is successful or not, you will have  a trace in the logs that tells you what went wrong, so that you can debug.
   If your job does not kick off ,and you see no graph at all, then you have problems as it means that there's something fundamentally wrong . I'd look informations 
   these places to figure out why
    5.1 Dockerfile. Are you copying the right files>
    5.2 image_spec.json.Make sure the image declared there is the same you use in your commands to create the flex templates
    5.3 spec\python_command_spec*.json: make sure the correct main file is instantiated

Hope this will help in getting you started.
