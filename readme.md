# Readme:

- [Readme:](#readme)
- [Setup](#setup)
  - [Connections:](#connections)
    - [Create a Virtual Env](#create-a-virtual-env)
    - [Get DEV Access:](#get-dev-access)
    - [Password Encryption](#password-encryption)
    - [**Databases**](#databases)
      - [**HERCCRT**](#herccrt)
      - [**Mosaic**](#mosaic)
      - [**YM_HIST**](#ym_hist)



# Setup

This setup would be specially helpful for the new hires.
(It is summary of my struggles in away :D)

## Connections:

If it is your first time running the code, please follow the instruction to connect each of the databases.

**Refer to the [new-hire doc](https://mysite.aa.com/:w:/g/personal/762066_corpaa_aa_com/Ef6LeiQngIpIt2BoXCyl-9wBWC8KCkGC73Ji_a39KFToCw?e=gxbULQ) for more details of each of the following process. Also follow the Tips mentioned here, they might (will) come handy.**

### Create a Virtual Env 

Use Anaconda (it's recommended here at AA, unless things have changes.) to create a virtual-env. All the packages to run this notebook are located in the `requirements.txt`. You can see the main ones here:


    config==1.0 -> Based on [Password Encyption - see bellow].
    jupyterlab==3.4.6
    matplotlip==0.2
    murph==1.0
    pip==22.1.2
    pipdeptree==2.3.1
    pretzels==1.0
    seaborn==0.12.0
      - matplotlib [required: >=3.1, installed: 3.5.3]
      - numpy [required: >=1.17, installed: 1.23.2]
      - pandas [required: >=0.25, installed: 1.4.4]
    sklearn==0.0
    tensorflow==2.10.0
    wincertstore==0.2

**Tip**: If the firewall prevents you from creating a virtual environment in the office, you can either use your phone as a hotspot or create the environment at home.

### Get DEV Access:
You need dev access.

Submit your access using [this link](http://zerotouch2.corpaa.aa.com/SoftwareServices/SitePages/Devaccess.aspx). 

**Tip**: If it took more than a day or two for your request to be approved, visit the IT help desk and speak with an IT representative there. It will speed up the process.


### Password Encryption

Follow the [installation process](https://github.com/AAInternal/python_pkgs-local#installation) to encrypt your password. Follow the Azure instruction in if you want to use Azure Databricks (Get Token and Cluster configs from Dtabricks and add them to ODBC - you might need to download the [Databricks ODBC Driver](https://www.databricks.com/spark/odbc-drivers-download)).

**Tip**: Make sure you use these names in ODBC configuration:
- For Mosaic: `MOSAIC_PROD`
- For Azure: `Azure-RMRCH-ADBAH-prod`

**TIP**: If you change your password, rerun this:
```
from murph import encrypt
encrypt()
```

---------

### **Databases**

- [HERCCRT](#herccrt)
- [Mosaic](#mosaic)
- [YM_HIST](#ymhist)

#### **HERCCRT**

You will need access to `Oracle SQL` software, and `HERCCRT` data:

1. To access Oracle, you need the follwoings:
    a. Dev access
    b. Send email to `Kelly Tremaine - Kacprowski@aa.com` to get access to the shared drives. 
    c. Set up your GitHub account - refer to new-hire doc.
    d. After a and b, copy the folder `O:\RMDEPT\OR\SQL_Developer_4` to `C:\Program Files\` 

2. Request for [HERCCRT data](https://github.com/AAInternal/HERC-Data_Dictionary).

**Tip**: If your shared drive access didn't went trhough fast enough, ask your manager to contact them directly.

**Tip**: When running the code, if you got to problems related to `C:\ORACLE` folder, contact Wendy (wendy.murdy@aa.com).


#### **Mosaic**

You will need access to `Teradata` software, and `Mosaic` data:

1. You need to first get `Teradata` running. Request the latest Teradata (`16.20` as of September 12. 2022) using [software request](http://zerotouch2.corpaa.aa.com/SoftwareServices/softwarerequest.aspx) platform.

2. Use [this instruction](https://wiki.aa.com/bin/view/Orion/Mosaic/How%20to%20Request%20MOSAIC%20Production%20Access/) to request the Mosaic data. For this Notebook, make sure to request for: 
    - `PROD_INDSTR_FLIGHT_SCHD_VW` : Industry Data Subject area (PROD_INDSTR_FLIGHT_SCHD_VW)  
    - `MIRS` : MIRS_1-Marketing Information System: For all MIRS users(PROD_MIRS_VW, PROD_MIRS_VWS, PROD_MIRSCTL_VWS, PROD_MIRSAUDIT_VW)
    - `PROD_REFERENCE_DATA_VWS` : :Reference data (PROD_REFERENCE_DATA_VWS)


#### **YM_HIST**

You will need `Databricks` access, and request for the `YM_HIST`.

1. To connect to Databricks, follow the instruction in this [GitHub page](https://github.com/AAInternal/TUTORIAL-Databricks#requesting-access).
2. Create a "RMHRC cluster access" issue under https://github.com/AAInternal/RPT-DEA/issues.
   
    `RMHRC` cluster access comes with access to `RMSBX`, `ym_hist`, and the `RM` fileshare by default. No need to make a separate request.