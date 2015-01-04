#/usr/bin/python
# -*- coding: utf-8
import json
from osgeo import ogr,gdal 
import threading
import os
import subprocess
import time
import numpy
import urllib
import shapely.wkt
import shapely.geometry
import sys
import shutil

class CompoShp:
	def __init__(self, path_shp):
		self.shp = ogr.Open(path_shp)
		self.layer = self.shp.GetLayerByIndex(0)
		self.extent = self.layer.GetExtent()
		self.zones = []
		for feature in self.layer:
			geom = feature.GetGeometryRef()
			self.zones.append(shapely.wkt.loads(geom.ExportToWkt()))
		print "Polygones destination : ",len(self.zones)


class ThreadComposite(threading.Thread):
	def __init__(self, thread_id, x, y, pas, layers, res, n_bande, compo):
		threading.Thread.__init__(self)
		self.thread_id = thread_id
		self.x = x
		self.y = y
		self.pas = pas
		self.layers = layers
		self.res = res
		self.n_bande = n_bande
		self.compo = compo
	
	def run(self):
		if not os.path.exists(self.thread_id):
			os.mkdir(self.thread_id)

		width = height = self.pas/self.res
		dest_file = "%s/composite_%d_%d.tif"%(self.thread_id, self.x, self.y)
		if os.path.exists("sortie/"+os.path.basename(dest_file)):
			return True
		if os.path.exists("sortie/"+os.path.basename(dest_file)+".vide"):
			return True
		
		## Test si c'est pas une tuile vide
		tuile = shapely.geometry.Polygon([(self.x,self.y), (self.x,self.y+self.pas), (self.x+self.pas,self.y+self.pas), (self.x+self.pas,self.y), (self.x,self.y)])
		tuile_vide = True
		for zone in self.compo.compo.zones:
			if zone.intersects(tuile):
				tuile_vide = False
	
		if tuile_vide:
			print self.thread_id, "Passe aucune ortho (1)"
			f = open("sortie/"+os.path.basename(dest_file)+".vide","w")
			f.close()
			return True

		## Rasterize le shape de composition
		cmd = "gdal_rasterize -tr %F %F -a id -te %d %d %d %d -ot Char %s %s/merge_2154.tif"%(self.res,self.res*-1,self.x,self.y,self.x+self.pas,self.y+self.pas,self.compo.conf["shapefile"],self.thread_id)
		subprocess.call(cmd, shell=True)
		driver = driver = gdal.GetDriverByName("GTiff")
		dsrc = gdal.Open(self.thread_id+"/merge_2154.tif")
		dsrc_band = dsrc.GetRasterBand(1)
		dsrc_data = dsrc.ReadAsArray(0,0,dsrc_band.XSize,dsrc_band.YSize)
		liste = numpy.unique(dsrc_data)
		nombre_layer = 0
		for layer_n in liste:
			if layer_n > 0:
				nombre_layer+=1

		if nombre_layer == 0:
			print self.thread_id, "Passe aucune ortho (2)"
			f = open("sortie/"+os.path.basename(dest_file)+".vide","w")
			f.close()
			return True

		dest = driver.Create(dest_file, int(self.pas/self.res), int(self.pas/self.res), self.n_bande+1, gdal.GDT_Byte)
		dest.SetGeoTransform(dsrc.GetGeoTransform())

		# ajout de chaque couche presente dans dest
		for layer_n in liste:
			layer_n = int(layer_n)
			if layer_n < 1:
				continue
			layer = self.layers[str(layer_n)]
			basename = "%d_%d_l%s" % (self.x/self.pas,self.y/self.pas,layer_n)
			fichier_tmp = self.compo.wms_query(layer,"%d,%d,%d,%d" % (self.x,self.y,self.x+self.pas,self.y+self.pas) ,width,height)
			print fichier_tmp, self.thread_id+"/"+basename+".png"
			#os.rename(fichier_tmp,self.thread_id+"/"+basename+".png")
			shutil.move(fichier_tmp,self.thread_id+"/"+basename+".png")
			
			# on s'assure du format du png
			cmd = "convert %s -define png:color-type=6 %s/my.png"%(self.thread_id+"/"+basename+".png", self.thread_id)
			print self.thread_id, cmd
			subprocess.call(cmd, shell=True)
			os.unlink(self.thread_id+"/"+basename+".png")

			# on le transforme en tiff
			cmd = "gdal_translate -a_ullr %d %d %d %d -a_srs EPSG:2154 -of GTiff %s.png %s.tif" % (self.x,self.y,self.x+self.pas,self.y+self.pas,self.thread_id+"/my",self.thread_id+"/"+basename)
			print self.thread_id, cmd
			subprocess.call(cmd, shell=True)
			os.unlink(self.thread_id+"/my.png")

			wms_src = gdal.Open("%s/%s.tif"%(self.thread_id,basename))
			for band in range(1,self.compo.conf['n_bande']+1):
				print self.thread_id, "Bande",band,"layer",layer_n,"..."
				wms_band = wms_src.GetRasterBand(band)
				wms_data = wms_band.ReadAsArray(0,0,dsrc_band.XSize,dsrc_band.YSize)
				dest_band = dest.GetRasterBand(band)
				dest_data = dest_band.ReadAsArray(0,0,dsrc_band.XSize,dsrc_band.YSize)
				dest_data += (dsrc_data*(dsrc_data==layer_n)/layer_n)*wms_data
				dest_band.WriteArray(dest_data)
				dest_data = None
				dest_band = None
				wms_data = None
				wms_band = None
			os.unlink(self.thread_id+"/"+basename+".tif")

			# transparence
			print self.thread_id, "transparence"
			dest_band = dest.GetRasterBand(self.compo.conf["n_bande"]+1)
			dest_data = dest_band.ReadAsArray(0,0,dsrc_band.XSize,dsrc_band.YSize)
			dest_data = (255*(dsrc_data!=0))
			dest_band.WriteArray(dest_data)
			dest_data = None
			dest_band = None	
		os.unlink(self.thread_id+"/merge_2154.tif")
		dsrc = None
		dest = None
		
		if self.compo.conf["n_bande"] > 1:
			cmd = "gdal_translate %s sortie/%s -b 1 -b 2 -b 3 -mask 4 -co COMPRESS=JPEG -co PHOTOMETRIC=YCBCR -co JPEG_QUALITY=80 --config GDAL_TIFF_INTERNAL_MASK YES" % (dest_file,os.path.basename(dest_file))
		else:
			cmd = "gdal_translate %s sortie/%s -b 1 -mask 2 -co COMPRESS=LZW --config GDAL_TIFF_INTERNAL_MASK YES" % (dest_file,os.path.basename(dest_file))
		print "Compression :", cmd
		subprocess.call(cmd, shell=True)
		os.unlink(dest_file)

class Composite:
	def __init__(self):
		f = open("config.json","r")
		json_str = f.read()
		f.close()
		self.conf = json.loads(json_str)

		self.compo = CompoShp(self.conf['shapefile'])
		
		pas = self.conf['pas']

		self.x0 = int(self.compo.extent[0]/pas-1)*pas
		self.x1 = int(self.compo.extent[1]/pas+1)*pas
		self.y0 = int(self.compo.extent[2]/pas)*pas
		self.y1 = int(self.compo.extent[3]/pas+1)*pas

		self.wms_query_dispo = True
		self.main_loop()

	def main_loop(self):
		x = self.x0
		y = self.y0
		threads = []
		while x <= self.x1:
			while y <= self.y1:
				suivant = False
				while not suivant:
					if len(threads) < self.conf["n_thread_max"]:
						tid = "thread_%d" % (len(threads)+1)
						suivant = True
					else:
						for t in threads:
							if t.is_alive():
								continue
							tid = t.thread_id
							threads.remove(t)
							suivant = True
							break
					if suivant:
						thread = ThreadComposite(tid,x,y,self.conf["pas"],self.conf["layers"],self.conf["resolution"],self.conf["n_bande"],self)
						thread.start()
						threads.append(thread)
					else:
						time.sleep(0.01)
				y += self.conf["pas"]
			x += self.conf["pas"]
			y = self.y0
		print "Terminé"
	
	def wms_query(self, layer, bbox, width, height):
		while not self.wms_query_dispo:
			time.sleep(2)
			#print "\ten attente wms dispo"
		self.wms_query_dispo = False
		args = {
			"BBOX": bbox,
			"WIDTH": "%d"%(width),
			"HEIGHT": "%d"%(height),
			"FORMAT": "image/png",
			"SRS": "EPSG:2154",
			"VERSION": "1.1.1",
			"REQUEST": "GetMap",
			"SERVICE": "WMS"
		}
		url = "%s&%s"%(layer,urllib.urlencode(args))
		print "download", url
		fichier, entetes = urllib.urlretrieve(url)
		print entetes
		print fichier
		self.wms_query_dispo = True
		return fichier

Composite()
	
