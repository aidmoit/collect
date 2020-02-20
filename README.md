# Populate Data Lake
The populating is done by using two scripts writing in python and R. These two languages have to be used because the functionalities offered by their libraries were of an unequal level of quality. Indeed, python offers an excellent library to interact with hdfs, while R has interesting modules to manage iso19115 metadata. In order to reduce the complexity generated by the concomitant use of these two languages, the R script has been encapsulated inside the python script. Thus, the administrator only needs to run the python script.

# Run the script 
```shell
python3 src/main.py
```

## Python : collect data and insert to Data Lake on data zone : HDFS Cluster
### Prerequisites
Has to be run on python 3.6 with requirements found in python-requirements.txt
If you are using pip, you can install all requirement with this command :
```shell
pip3 install -r python-requirements.txt
```
### Files
* Main script : **src/main.py**
* This script read information from a csv file : input/datasources.csv
* It gives 2 kind of output :
	+ **/output/data/** : Files downloaded 
	+ **/output/meta/meta.json** : a json file containing metadata collected on Internet

## R script : Insert into Data Lake metadatat management system : GeoNetwork
### Prerequisites
This script needs to use the Geonetwork's API. You have to enable it it your geonetwork settings. 
cf : https://github.com/geonetwork/core-geonetwork/blob/9310d0ba85e6a35f48dbfa5d6168ba7088609724/web/src/main/webapp/WEB-INF/config/config-service-xml-api.xml#L83
*Pay attention that if Geonerwork is updated, you may lose this setting specially if you are using a tomcat war (that's the case for Geosur).*

### Files :
* Main script : **src/addServicesToGN.R**
* This script read informations from 2 files :
 	+ **input/tetis-services.csv**    
 	+ **output/meta/meta.json** : Json file generated by the main python script containing informations for building HDFS path link

### Dependencies
It only uses 2 librairie from FAO :
* geonapi : R librairy to insert/update/delete metadata directly in our geonetwork : https://github.com/eblondel/geonapi
* geometa : R librairy to create iso 19115 xml (inspire compliant) : https://github.com/eblondel/geometa

### Install
* Dependencies System libs:
```shell
sudo apt-get install libssl-dev libxml2-dev
```
* Dependencies System lib related to R software:
```shell
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo add-apt-repository 'deb [arch=amd64,i386] https://cran.rstudio.com/bin/linux/ubuntu xenial/'
sudo apt-get install r-base
```
* R addiotional libs:
	(see https://github.com/eblondel/geonapi/wiki#install_guide, https://github.com/eblondel/geometa/wiki#install_guide)
```shell
install.packages("devtools")
install.packages("XML")
install.packages("uuid")
install.packages("osmdata")
install.packages("rjson")
require("devtools")
install_github("eblondel/geometa")
install_github("eblondel/geonapi")
```
## License
![licence](https://img.shields.io/badge/Licence-GPL--3-blue.svg)
**Aidmoit's Collect** is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.


This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.


You should have received a copy of the GNU General Public License
along with this program.  If not, see http://www.gnu.org/licenses/

## Authors
UMR TÉTIS  / IRSTEA
