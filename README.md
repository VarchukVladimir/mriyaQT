# mriyaQT


## Installation:
### Prerequisites:
Ubuntu 16.04
sudo apt-get install python2.7-dev libgl1-mesa-dev libsdl2-2.0-0 libsdl2-image-2.0-0 libsdl2-mixer-2.0-0 libsdl2-ttf-2.0-0

### Virtual environment:
`cd`<br>
`mkdir virtualenv`<br>
`sudo pip install virtualenv`<br>
`cd virtualenv/`<br>
`virtualenv datamigration`<br>
`echo "export PYTHONPATH=~/git/mriyaQt/src/salesforce_bulk__/:." >> ~/virtualenv/datamigration/bin/activate`<br>
`cd ../git/`<br>

### Source code && Libraies:
git clone https://github.com/VarchukVladimir/mriyaQT.git
source ~/virtualenv/datamigration/bin/activate
pip install requirements.txt

### Config file && credentials:
Create configfile. Each connection should have unique name. As example use file config template.ini. Parameter Threads Should be 0.<br>
```
    [src]
    consumer_key = 
    consumer_secret = 
    username = 
    password = 
    ; use only host prefix. For exmaple if host is admin.test.salesforce.com use admin.test.
    host_prefix = 
    threads = 0
    secret_token = ;fill it out if your IP address is not in WhiteList in Salesforce
```	
```
	[dst]
    consumer_key = 
    consumer_secret = 
    username = 
    password = 
    ; use only host prefix. For exmaple if host is admin.test.salesforce.com use admin.test.
    host_prefix = 
    threads = 0
    secret_token = ;fill it out if your IP address is not in WhiteList in Salesforce
```
## Execeute

python src/mriya_qt.py config.ini


everything tested on Ubuntu 14.04, 16.4
