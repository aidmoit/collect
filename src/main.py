#!/usr/bin/env python

"""
AIDMOIt : Collect data and insert to HDFS cluster

Step :
    1/ Browse CSV file
    2/ Download File
    3/ Insert in HDFS cluster
    4/ Build ISO19139 XML describing metadata
    5/ Insert ISO19139 in geonetwork
"""
import os
import re
import urllib.request
import pandas as pd
import requests
import json
from pywebhdfs.webhdfs import PyWebHdfsClient
import subprocess


def getUrlFromOpendata3M(inputCSV):
    """
    Data from 3M opendata website are collected in 4 steps :
        1/ Parse CSV
        2/ From this file, get all links to 3M opendata website
        3/ For each link get the 3M ID of dataset
        4/ For each 3M dataset's ID get url asking 3M opendata's API:
            metadata
            data
    :return:dictionnary with id node of 3M opendata dataset as key and a dictionnary as value containing data & metadata
        Exemple of return :
        {"[u'9795']": {'data': [u'http://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_PduHierarchieVoies.zip', u'http://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_PduHierarchieVoies_geojson.zip', u'http://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_PduHierarchieVoies.ods', u'http://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_PduNotice.pdf'], 'metadata': [TO BIG TO PRINT for this example!!!]},
        "[u'3413']": {'data': [u'http://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_OccupationSol.zip', u'http://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_OccupSol_Lyr.zip', u'http://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_OccupSol_Nomenclature_2018.pdf', u'http://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_OccupationSol_Archives.zip'], 'metadata': [TO BIG TO PRINT for this example!!!]},
        " [u'9860']": {'data': [u'http://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_PopFine.zip', u'http://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_PopFine_Description.docx', u'http://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_PopFine_Schema.docx'], 'metadata': [TO BIG TO PRINT for this example!!!]},
    """
    # Step 1 and 2
    dataInvetoryFile = pd.read_csv(inputCSV, sep = ';')
    weblinks = dataInvetoryFile['datasetURL']

    # Step 3
    idNodePattern = re.compile("https{0,1}:..data.montpellier3m.fr.node.(\d+)")
    idNodeList = []
    for weblink in weblinks:
        html = requests.get(weblink)
        idNodeList.append(re.findall(idNodePattern, html.text))

     # Step 4
    opendata3mDataMetada = {}
    idcsv = 1
    for node in idNodeList:
        opendata3mData = []
        nodeDataMetada = {'metadata': None, 'data': None, 'idCSV': idcsv}
        metadata = requests.get("http://data.montpellier3m.fr/api/3/action/package_show?id="+node[0]).json()
        #get resources
        for resource in metadata['result']['resources']:
            opendata3mData.append(resource['url'])
        nodeDataMetada['data'] = opendata3mData
        nodeDataMetada['metadata'] = metadata
        opendata3mDataMetada.update({str(node): nodeDataMetada})
        idcsv = idcsv+1

    return opendata3mDataMetada


def downloadOpendata3MFiles(opendata3mDataMetada, pathToSaveDownloadedData):
    """
    Download all resources given

    :param opendata3mDataMetada: dictionary containing metadata and data to download by Id node from 3M opendata
    :return: None
    """
    nboffiledl = 0
    for node in opendata3mDataMetada:
        for fileToDownoald in opendata3mDataMetada[node]['data']:
            urllib.request.urlretrieve(fileToDownoald, os.path.join(pathToSaveDownloadedData, fileToDownoald.split('/')[-1]))
            nboffiledl = nboffiledl + 1

    return nboffiledl


if __name__ == '__main__':
    #Init variables
    dirname = os.path.dirname(__file__)
    inputCSV = os.path.join(dirname, '../input/datasources.csv')
    pathToSaveDownloadedData = os.path.join(dirname, '../output/data')
    pathToSaveDownloadedMeta = os.path.join(dirname, '../output/meta/meta.json')
    pathToSaveHDFSPath = os.path.join(dirname, '../output/hdfspath/hdfspath.csv')
    nboffiledl = 0

    namenode = "namenode" # hostname or IP address for HDFS cluster's namenode
    namenodePort = "9870"
    hdfsuser = "hadoop"
    #end of init variables

    print("AIDMOIt ingestion module starts")

    """Get URL of data and metadata from 3M Opendata website"""
    opendata3mDataMetada = getUrlFromOpendata3M(inputCSV)
    jsonfile = open(pathToSaveDownloadedMeta, "w")
    jsonfile.write(json.dumps(opendata3mDataMetada))
    jsonfile.close()
    """Download File"""
    nboffiledl = downloadOpendata3MFiles(opendata3mDataMetada, pathToSaveDownloadedData)

    """Insert files inside HDFS and store file"""
    # connect to HDFS
    hdfs = PyWebHdfsClient(host=namenode, port=namenodePort, user_name=hdfsuser)
    for file in os.listdir(pathToSaveDownloadedData):
        if(str(file) != ".forgit"):
            try:
                pathInDL = "."
                file_data = str(file)
                hdfs.create_file(file_data, pathInDL)
            except Exception as e:
                print('Failed to upload in HDFS: '+ str(e))

    """Build and insert iso19139 xml to geonetwork"""
    try:
        subprocess.call("/usr/bin/Rscript  addServicesToGN.R")
    except :
        print("R error due to OSM ? Try re-launched")
        subprocess.call("R -f addServicesToGN.R", shell=True)

    print(str(nboffiledl)+" files downloaded in : " + pathToSaveDownloadedData)
    print("AIDMOIt ingestion module ends")
