"""
github: https://github.com/brandonxiang/pyMap
license: MIT
"""
# -*- coding: utf-8 -*-
# coding=utf-8
import os
import sys
import math
import requests
from tqdm import trange
import configparser
import threading
import datetime
import threadpool

URL = {
    "gaode": "http://webrd02.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=7&x={x}&y={y}&z={z}",
    "gaode8": "http://webrd02.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=8&x={x}&y={y}&z={z}",
    "gaode.image": "http://webst02.is.autonavi.com/appmaptile?style=6&x={x}&y={y}&z={z}",
    "tianditu": "http://t2.tianditu.cn/DataServer?T=vec_w&X={x}&Y={y}&L={z}",
	  "tianditu_cva_w" : "https://t1.tianditu.gov.cn/DataServer?T=cva_w&x={x}&y={y}&l={z}&tk=[token]",
	  "tianditu_vec_w" : "https://t1.tianditu.gov.cn/DataServer?T=vec_w&x={x}&y={y}&l={z}&tk=[token]",
    "googlesat": "http://khm0.googleapis.com/kh?v=203&hl=zh-CN&&x={x}&y={y}&z={z}",
    "tianditusat":"http://t2.tianditu.cn/DataServer?T=img_w&X={x}&Y={y}&L={z}",
    "esrisat":"http://server.arcgisonline.com/arcgis/rest/services/world_imagery/mapserver/tile/{z}/{y}/{x}",
    "gaode.road": "http://webst02.is.autonavi.com/appmaptile?x={x}&y={y}&z={z}&lang=zh_cn&size=1&scale=1&style=8",
    "default":"http://61.144.226.124:9001/map/GISDATA/WORKNET/{z}/{y}/{x}.png",
    "szbuilding":"http://61.144.226.124:9001/map/GISDATA/SZBUILDING/{z}/{y}/{x}.png",
    "szbase":"http://61.144.226.44:6080/arcgis/rest/services/basemap/szmap_basemap_201507_01/MapServer/tile/{z}/{y}/{x}",
    "google.topography":"http://mt1.google.cn/vt/lyrs=t@131,r@227000000&hl=zh-CN&gl=cn&x={x}&y={y}&z={z}&scale=1",
    "google.satellite":"http://www.google.cn/maps/vt?lyrs=s@821&gl=cn&x={x}&y={y}&z={z}"
}


f1 = open('log-download.txt','a')
processRowList = []

def latlng2tilenum(lat_deg, lng_deg, zoom):
    """
    convert latitude, longitude and zoom into tile in x and y axis
    referencing http://www.cnblogs.com/Tangf/archive/2012/04/07/2435545.html

    Keyword arguments:
    lat_deg -- latitude in degree
    lng_deg -- longitude in degree
    zoom    -- map scale (0-18)

    Return two parameters as tile numbers in x axis and y axis
    """
    n = math.pow(2, int(zoom))
    xtile = ((lng_deg + 180) / 360) * n
    lat_rad = lat_deg / 180 * math.pi
    ytile = (1 - (math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi)) / 2 * n
    return math.floor(xtile), math.floor(ytile)

def process_latlng(north, west, south, east, zoom, output, maptype="default", directDownload=True):
    """
    download and mosaic by latlng

    Keyword arguments:
    north -- north latitude
    west  -- west longitude
    south -- south latitude
    east  -- east longitude
    zoom  -- map scale (0-18)
    output -- output file name

    """
    north = float(north)
    west = float(west)
    south = float(south)
    east = float(east)
    zoom = int(zoom)
    assert(east>-180 and east<180)
    assert(west>-180 and west<180)
    assert(north>-90 and north<90)
    assert(south>-90 and south<90)
    assert(west<east)
    assert(north>south)

    left, top = latlng2tilenum(north, west, zoom)
    right, bottom = latlng2tilenum(south, east, zoom)
    process_tilenum(left, right, top, bottom, zoom, output, maptype, directDownload)


def process_tilenum(left, right, top, bottom, zoom, output, maptype="default", directDownload=True):
    """
    download and mosaic by tile number

    Keyword arguments:
    left   -- left tile number
    right  -- right tile number
    top    -- top tile number
    bottom -- bottom tile number
    zoom   -- map scale (0-18)
    output -- output file name

    """
    left = int(left)
    right = int(right)
    top = int(top)
    bottom = int(bottom)
    zoom = int(zoom)
    assert(right>=left)
    assert(bottom>=top)

    #download(left, right, top, bottom, zoom, output, maptype)
    for y in range(top, bottom + 1):
        processRowList.append(([left,right,y,zoom,output,maptype],None))
        if directDownload:
            _downloadRow(left, right, y, zoom, output, maptype)


def download(left, right, top, bottom, zoom, output, maptype="default"):
    
    for x in range(left, right + 1):
        for y in range(top, bottom + 1):
            path = getArcGISCacheTypeFile(output, zoom, y, x)
            if not os.path.exists(path):
                _download(x, y, zoom, output, maptype)


def _downloadRow(minCol, maxCol, row, level, folder, maptype="default"):
    url = URL.get(maptype, maptype)

    rowPath = getArcGISCacheTypePath(folder, level, row)
    if not os.path.isdir(rowPath):
        os.makedirs(rowPath)
    
    for col in range(minCol, maxCol + 1):
        tileFile = getArcGISCacheTypeFile(folder, level, row, col)
        if not os.path.exists(tileFile):
            map_url = url.format(x=col, y=row, z=level)
            r = requests.get(map_url)
            with open(tileFile, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        f.flush()


def _download(x, y, z, output, maptype):
    url = URL.get(maptype, maptype)
    path = getArcGISCacheTypePath(output, z, y)
    map_url = url.format(x=x, y=y, z=z)
    r = requests.get(map_url)
    
    if not os.path.isdir(path):
        os.makedirs(path)
    with open(getArcGISCacheTypeFile(output, z, y, x), 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()
                
def getArcGISCacheTypeFile(folder, level, row, col):
    path = getArcGISCacheTypePath(folder, level, row)
    colName = "C%08X" % col
    return '%s/%s.png' % (path, colName)

def getArcGISCacheTypePath(folder, level, row):
    levelName = "L%02d" % level
    rowName = "R%08X" % row
    return './tiles/%s/_alllayers/%s/%s' % (folder, levelName, rowName)


def downloadByConfig(config):
    cf = configparser.ConfigParser()
    cf.read(config, encoding="utf-8")
    download = cf.get("config","下载方式")
    left = cf.get("config","最左")
    top = cf.get("config","最上")
    right = cf.get("config","最右")
    bottom = cf.get("config","最下")
    zooms = cf.get("config","级别")
    name = cf.get("config","保存目录")
    maptype = cf.get("config","地图地址")

    zoomArray = zooms.split(',')
    
    f1.write("\n\n————————————————————————")
    f1.write("\n【瓦片数据下载开始！" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "】【" + config + "】")
    
    for zoom in zoomArray:
        if download == "瓦片编码":
            process_tilenum(left,right,top,bottom,zoom,name,maptype,True)
        elif download == "地理编码":
            process_latlng(top,left,bottom,right,zoom,name,maptype,True)
        print("\n第" + zoom + "级下载完成！")
        f1.write("\n第" + zoom + "级下载完成！")

    f1.write("\n【瓦片数据下载完成！" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "】")
    f1.write("\n\n————————————————————————")

def downloadByConfigMultiThread(config):
    cf = configparser.ConfigParser()
    cf.read(config, encoding="utf-8-sig")
    download = cf.get("config","下载方式")
    left = cf.get("config","最左")
    top = cf.get("config","最上")
    right = cf.get("config","最右")
    bottom = cf.get("config","最下")
    zooms = cf.get("config","级别")
    name = cf.get("config","保存目录")
    maptype = cf.get("config","地图地址")

    zoomArray = zooms.split(',')
    
    for zoom in zoomArray:
        if download == "瓦片编码":
            process_tilenum(left,right,top,bottom,zoom,name,maptype,False)
        elif download == "地理编码":
            process_latlng(top,left,bottom,right,zoom,name,maptype,False)

    pool = threadpool.ThreadPool(10)
    requests = threadpool.makeRequests(_downloadRow, processRowList)
    [pool.putRequest(req) for req in requests]
    pool.wait()


def test():
    process_tilenum(803,857,984,1061,8,'WORKNET')

if __name__ == '__main__':
    downloadByConfigMultiThread("mtd_config.conf")
    

    

