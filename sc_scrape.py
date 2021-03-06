from __future__ import unicode_literals
from __future__ import print_function # Python 2/3 compatibility
import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import decimal
import os
import math
import sys
import random
from datetime import datetime
from datetime import date
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import Firefox
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
import time
import arrow
import urllib.request
import fire

import subprocess
import re

import youtube_dl
from mutagen.mp3 import MP3

import init_artists
import refresh_lib as rl

base_fs_dir = (os.path.dirname(os.path.realpath(__file__)) + '/')

def download_all_new_links():

	base_dir = base_fs_dir + 'staging/'

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	url_response = table.scan(FilterExpression=Attr('downloaded').eq("false")&Attr('uploaded').eq("false"))	
	urls_to_dl = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'],FilterExpression=Attr('downloaded').eq("false")&Attr('uploaded').eq("false"))
		urls_to_dl.extend(url_response['Items'])

	print ("all urls to download now")
	for url_row in urls_to_dl:
		print(url_row['url_link'])
	
	print('Number of files to download:' + str(len(urls_to_dl)))
	
	for url_row in urls_to_dl:
		try:
			download_one_track(url_row)		
		except Exception as e:
			print(e)

	return

def download_upload_all_new_links(platform_to_dl = 'youtube'):

	base_dir = base_fs_dir + 'staging/'
	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')


    #temporarily only downloading the youtube sets since soundcloud links seem to be not working 
	url_response = table.scan(FilterExpression=Attr('downloaded').eq("false")&Attr('uploaded').eq("false")&Attr('platform').eq(platform_to_dl))	
	urls_to_dl = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'],FilterExpression=Attr('downloaded').eq("false")&Attr('uploaded').eq("false")&Attr('platform').eq(platform_to_dl))
		urls_to_dl.extend(url_response['Items'])

	print ("all urls to download now")
	for url_row in urls_to_dl:
		#print(url_row)
		print(url_row['url_link'])
	
	print('Number of files to download:' + str(len(urls_to_dl)))
	
	#In case of using multiple downloaders, randomise order
	random.shuffle(urls_to_dl)

	successful_downloads = []
	unsuccessful_downloads = []

	for url_row in urls_to_dl:
		try:
			download_one_track(url_row)
			s3upload_single_track(url_row)
			successful_downloads.append(url_row)

		except Exception as e:
			print(e)
			unsuccessful_downloads.append(url_row)

	if (len(unsuccessful_downloads) > 0):
		email_body = "successful downloads\n"
		email_body += str([str(f"{x['url_link']} {x['title']}") for x in successful_downloads])
		email_body += "\n####################################\n"
		email_body += "unsuccessful downloads\n"
		email_body += str([str(f"{x['url_link']} {x['title']}") for x in unsuccessful_downloads])
		msg_client = boto3.client('sns',region_name='us-west-2')
		topic = msg_client.create_topic(Name="crypto-news-daily")
		topic_arn = topic['TopicArn']  # get its Amazon Resource Name
		mail_subject = 'Freeform-scrape-failure-report'
		msg_client.publish(TopicArn=topic_arn,Message=email_body,Subject="unsuccessful downloads")

	return

def download_one_track(url_row):

	base_dir = base_fs_dir + 'staging/'
	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	#check if already downloaded by inprogress process
	check_resp = table.get_item(Key ={'url_link' : url_row['url_link']})
	check_item = check_resp['Item']
	if check_item['downloaded'] == 'true':
		print('Already downloaded in the meantime,')
		return

	try:
		url = url_row['url_link']
		platform = url_row['platform']
		artist = url_row['artist']
		title = url_row['title']
		print("\nattempting download of:" + url)
		ext = ""

		# Perform the download 
		if (platform == 'youtube'):
			youtube_ydl_opts  = {
				'format': 'bestaudio/best',
				'outtmpl': base_fs_dir + 'staging/[%(uploader)s]%(title)s.%(ext)s',
				'writedescription': True,
				'postprocessors': [{
					'key': 'FFmpegExtractAudio',
					'preferredcodec': 'mp3',
					'preferredquality': '192',
				}],
			}
			with youtube_dl.YoutubeDL(youtube_ydl_opts) as ydl:
				
				info_dict = ydl.extract_info(url, download=False)
				filename = ydl.prepare_filename(info_dict)
				name_only = filename[len(base_dir):].rsplit('.',1)[0]
				ext = filename.rsplit('.')[-1:][0]
				print('FILE IS:' + name_only)
				result = ydl.download([url])
				print("downloaded:" + url)

				# Update the table if download was successful
				table.put_item(
						Item={
							'url_link': url,
							'platform': platform,
							'artist': artist,
							'downloaded': 'true',
							'title' : title,
							'filename' : name_only,
							'uploaded' : 'false',
						},
					)
		if (platform == 'soundcloud'):
			soundcloud_ydl_opts = {
			'writedescription': True,
			'outtmpl': base_fs_dir + 'staging/[%(uploader)s]%(title)s.%(ext)s',
			'postprocessors': [{
					'key': 'FFmpegExtractAudio',
					'preferredcodec': 'mp3',
					'preferredquality': '192',
			 }],
			}
			with youtube_dl.YoutubeDL(soundcloud_ydl_opts) as ydl:
				info_dict = ydl.extract_info(url, download=False)
				filename = ydl.prepare_filename(info_dict)
				name_only = filename[len(base_dir):].rsplit('.',1)[0]
				ext = filename.rsplit('.')[-1:][0]
				print('FILE IS:' + name_only)
				ydl.download([url])
				print("downloaded:" + url)

				# Update the table if download was successful
				table.put_item(
						Item={
							'url_link': url,
							'platform': platform,
							'artist': artist,
							'downloaded': 'true',
							'title' : title,
							'filename' : name_only,
							'uploaded' : 'false',
						},
					)

		classify_single_track(url,ext);

	except Exception as e:
		print(e)


def download_information_only():	
	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	#Get download items
	url_response = table.scan()	
	all_urls = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'])
		all_urls.extend(url_response['Items'])



	print(f"total number of urls: {len(all_urls)}")
	downloaded_sets = [x for x in all_urls if x['downloaded'] == 'true']
	non_downloaded_sets = [x for x in all_urls if x['downloaded'] != 'true']
	print(f"total number of downloaded urls: {len(downloaded_sets)}")
	print(f"total number of non downloaded urls: {len(non_downloaded_sets)}")

	for url in non_downloaded_sets:
		print(f"{url['url_link']} {url['artist']} downloaded:{url['downloaded']}")

	#import pdb; pdb.set_trace()

	return

def get_S3_size_data():

	s3 = boto3.client('s3')
	# S3 Get set size info
	resp = s3.list_objects_v2(Bucket='freeform-scrape', Prefix='set')
	set_info = resp['Contents']

	while 'NextContinuationToken' in resp:
		resp = s3.list_objects_v2(Bucket='freeform-scrape',ContinuationToken=resp['NextContinuationToken'],Prefix='set')
		set_info.extend(resp['Contents'])

	total_set_size = 0
	for set in set_info:
		total_set_size += set['Size']
	total_set_size_GB = total_set_size/(math.pow(2,30))

	# S3 Get track size info
	resp = s3.list_objects_v2(Bucket='freeform-scrape', Prefix='track')
	track_info = resp['Contents']

	while 'NextContinuationToken' in resp:
		resp = s3.list_objects_v2(Bucket='freeform-scrape',ContinuationToken=resp['NextContinuationToken'],Prefix='track')
		track_info.extend(resp['Contents'])

	total_track_size = 0
	for track in track_info:
		total_track_size += track['Size']
	total_track_size_GB = total_track_size/(math.pow(2,30))

	# Get number of description files, should count exact number
	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	set_desc = 0
	url_response = table.scan(FilterExpression=Attr('description').eq("true")&Attr('classification').eq("set"))
	urls_to_dl = url_response['Items']	
	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'],FilterExpression=Attr('description').eq("true")&Attr('classification').eq("set"))
		urls_to_dl.extend(url_response['Items'])
	set_desc = len(urls_to_dl)

	track_desc = 0
	url_response2 = table.scan(FilterExpression=Attr('description').eq("true")&Attr('classification').eq("track"))
	urls_to_dlt = url_response2['Items']	
	while 'LastEvaluatedKey' in url_response2:
		url_response2 = table.scan(ExclusiveStartKey=url_response2['LastEvaluatedKey'],FilterExpression=Attr('description').eq("true")&Attr('classification').eq("track"))
		urls_to_dlt.extend(url_response2['Items'])
	track_desc = len(urls_to_dlt)

	S3_data_array = [
					str(len(track_info) - track_desc),
					str(round(total_track_size_GB,2)),
					str(len(set_info) - set_desc),
					str(round(total_set_size_GB,2))
					]

	return S3_data_array


def song_info_download():

	print('bulk download of information')
	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	url_response = table.scan(FilterExpression=Attr('description').ne("true")&Attr('uploaded').eq("true"))	
	urls_to_dl = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'],FilterExpression=Attr('description').ne("true")&Attr('uploaded').eq("true"))
		urls_to_dl.extend(url_response['Items'])

	print ("all urls to download now")
	for url_row in urls_to_dl:
		print(url_row['url_link'])
		song_info_download_upload_one_song(url_row)
	
	print('Number of files to get info:' + str(len(urls_to_dl)))

	return


def song_info_download_upload_one_song(url_row):
	base_dir = base_fs_dir + 'staging/'
	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	try:
		url = url_row['url_link']
		platform = url_row['platform']
		artist = url_row['artist']
		title = url_row['title']
		print("\nattempting download of:" + url)
		ext = ""

		# Perform the download 
		if (platform == 'youtube'):
			youtube_ydl_opts  = {
				'format': 'bestaudio/best',
				'outtmpl': base_fs_dir + 'staging/[%(uploader)s]%(title)s.%(ext)s',
				'writedescription': True,
				'skip_download': True,
				'postprocessors': [{
					'key': 'FFmpegExtractAudio',
					'preferredcodec': 'mp3',
					'preferredquality': '192',
				}],
			}
			with youtube_dl.YoutubeDL(youtube_ydl_opts) as ydl:
				
				info_dict = ydl.extract_info(url, download=False)
				filename = ydl.prepare_filename(info_dict)
				name_only = filename[len(base_dir):].rsplit('.',1)[0]
				ext = filename.rsplit('.')[-1:][0]
				print('FILE IS:' + name_only)
				result = ydl.download([url])
				print("downloaded:" + url)


		if (platform == 'soundcloud'):
			soundcloud_ydl_opts = {
			'writedescription': True,
			'skip_download': True,
			'outtmpl': base_fs_dir + 'staging/[%(uploader)s]%(title)s.%(ext)s',
			}
			with youtube_dl.YoutubeDL(soundcloud_ydl_opts) as ydl:
				info_dict = ydl.extract_info(url, download=False)
				filename = ydl.prepare_filename(info_dict)
				name_only = filename[len(base_dir):].rsplit('.',1)[0]
				ext = filename.rsplit('.')[-1:][0]
				print('FILE IS:' + name_only)
				ydl.download([url])
				print("downloaded:" + url)

	except Exception as e:
		print(e)

	s3 = boto3.resource('s3')
	old_url_row = url_row

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	#refresh file name from dynamodb table
	url_response = table.scan(FilterExpression=Attr('url_link').eq(old_url_row['url_link']))
	artists_response = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'],FilterExpression=Attr('url_link').eq(old_url_row['url_link']))
		artists_response.extend(url_response['Items'])

	artist_info = artists_response[0]
	url_row = artist_info

	try:
		url = url_row['url_link']
		filename = url_row['filename']
		classification = url_row['classification']
		print('Uploading Description:' + str(url))
		print(filename)

		# Upload a new file
		base_dir = base_fs_dir
		staging_file_description_location = (base_dir + 'staging/' + filename + '.description')

	except Exception as e:
		print(filename)
		print(e)

	try:
		data = open(staging_file_description_location, 'rb')
		print('uploading: ' + staging_file_description_location)
		s3.Bucket('freeform-scrape').put_object(Key=classification+ '/' + filename + '.description', Body=data)

		response = table.update_item(
		Key={
			'url_link': url,
		},
		UpdateExpression="set description = :r",
		ExpressionAttributeValues={
			':r': 'true',
		},
		ReturnValues="UPDATED_NEW"
		)
	except:
		print('description file not found - check staging area')

	# Remove from local storage (change so only occurs on succesful upload)
	if os.path.isfile(staging_file_description_location):
		os.remove(staging_file_description_location)
		print('file uploaded and removed from local system\n')

	else:    ## Show an error ##
		print("Error: %s file not found" % staging_file_description_location)

	return


def organise_staging_area():
	# to remove this function - replaced with classifier
	#check length of every file in staging area
	# if under <600 seconds, move to track, otherwise move to sets
	base_dir = base_fs_dir
	staging_tracks = os.listdir(base_dir + 'staging')

	for track in staging_tracks:
		staging_file_location = (base_dir + 'staging/' + track)
		try:
			audio = MP3(staging_file_location)
			track_length_seconds = audio.info.length
			print(track)
			print(track_length_seconds)

			if track_length_seconds > 600:
				print('set found')
				os.rename(staging_file_location, base_dir + 'sets/' + track)

			else:
				print ('track found')
				os.rename(staging_file_location, base_dir + 'tracks/' + track)

		except Exception as e:
			print("Failed reading length")

	return


def classify_single_track(link_to_classify, extension):

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	track = ""
	# figure out how to get the filename from the track name
	try:
	    response = table.get_item(
	        Key={
	            'url_link': link_to_classify,
	        }
	    )
	except ClientError as e:
	    print(e.response['Error']['Message'])
	else:
	    item = response['Item']
	    filename = response['Item']['filename']
	    print('filename is:' + filename)


	print(track)    
	#determine if its a track or set ( if file isn't found, update the downloaded flag to false)
	# Assume it is an mp3, as 95% are mp3s
	base_dir = base_fs_dir
	staging_file_location = (base_dir + 'staging/' + filename + '.mp3')
	try:
		audio = MP3(staging_file_location)
		track_length_seconds = audio.info.length
		print('length: '+ str(track_length_seconds))

		if track_length_seconds > 600:
			print('set found')
			classification ='set'

		elif track_length_seconds <= 600:
			print('track found')
			classification = 'track'
		else: 
			print('Non mp3 found')
			classification = 'TBA'

		response = table.update_item(
	    Key={
	        'url_link': link_to_classify,
	    },
	    UpdateExpression="set classification = :r",
	    ExpressionAttributeValues={
	        ':r': classification,
	    },
	    ReturnValues="UPDATED_NEW"
		)


	except Exception as e:
		print('probably not an MP3')
		if (extension == 'wav'):
			print('classifying wav file')
			staging_file_location = (base_dir + 'staging/' + filename + '.wav')

			try:
				process = subprocess.Popen(['ffmpeg',  '-i', staging_file_location], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
				stdout, stderr = process.communicate()
				matches = re.search(r"Duration:\s{1}(?P<hours>\d+?):(?P<minutes>\d+?):(?P<seconds>\d+\.\d+?),", stdout.decode(), re.DOTALL).groupdict()

				track_length_seconds = (float(matches['seconds']) + (60*float(matches['minutes'])) + (3600*float(matches['hours'])))

				print('length: '+ str(track_length_seconds))

				if track_length_seconds > 600:
					print('set found')
					classification ='set'

				elif track_length_seconds <= 600:
					print('track found')
					classification = 'track'
				else: 
					print('Non mp3 found')
					classification = 'TBA'

				response = table.update_item(
			    Key={
			        'url_link': link_to_classify,
			    },
			    UpdateExpression="set classification = :r",
			    ExpressionAttributeValues={
			        ':r': classification,
			    },
			    ReturnValues="UPDATED_NEW"
				)

			except Exception as e:
				print(e)
				response = table.update_item(
			    Key={
			        'url_link': link_to_classify,
			    },
			    UpdateExpression="set downloaded = :r",
			    ExpressionAttributeValues={
			        ':r': 'skip_u',
			    },
			    ReturnValues="UPDATED_NEW"
			    )


		else:
			response = table.update_item(
			    Key={
			        'url_link': link_to_classify,
			    },
			    UpdateExpression="set downloaded = :r",
			    ExpressionAttributeValues={
			        ':r': 'skip_u',
			    },
			    ReturnValues="UPDATED_NEW"
			    )

	return


def upload_to_s3():
	s3 = boto3.resource('s3')

	# for the summary email
	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	# get all links that are classified, downloaded, but not uploaded (add check for classifier)
	url_response = table.scan(FilterExpression=Attr('downloaded').eq("true")&Attr('uploaded').eq("false"))	
	urls_to_upload = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'],FilterExpression=Attr('downloaded').eq("true")&Attr('uploaded').eq("false"))
		urls_to_upload.extend(url_response['Items'])

	print('Files to upload:' + str(len(urls_to_upload)))
	
	for url_row in urls_to_upload:
		s3upload_single_track(url_row)


def s3upload_single_track(old_url_row):
	s3 = boto3.resource('s3')

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	#refresh file name from dynamodb table
	url_response = table.scan(FilterExpression=Attr('url_link').eq(old_url_row['url_link']))
	artists_response = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'],FilterExpression=Attr('url_link').eq(old_url_row['url_link']))
		artists_response.extend(url_response['Items'])

	artist_info = artists_response[0]
	url_row = artist_info

	try:
		url = url_row['url_link']
		filename = url_row['filename']
		classification = url_row['classification']
		print('Uploading:' + str(url))
		print(filename)

		# Upload a new file
		base_dir = base_fs_dir

		file_ex = '.mp3' # need to fix this properly
		staging_file_location = (base_dir + 'staging/' + filename + file_ex)
		staging_file_description_location = (base_dir + 'staging/' + filename + '.description')

		if(os.path.isfile(staging_file_location) == False):
			file_ex = '.wav'
			staging_file_location = (base_dir + 'staging/' + filename + file_ex)


	except Exception as e:
		print(filename)
		print(e)

	try:
		data = open(staging_file_location, 'rb')
		s3.Bucket('freeform-scrape').put_object(Key=classification+ '/' + filename + file_ex, Body=data)

		# confirm file isn't already present in S3 - if it is, skip the upload step.

		# upload, change DB setting to uploaded
		response = table.update_item(
		Key={
			'url_link': url,
		},
		UpdateExpression="set uploaded = :r",
		ExpressionAttributeValues={
			':r': 'true',
		},
		ReturnValues="UPDATED_NEW"
		)

		response = table.update_item(
		Key={
			'url_link': url,
		},
		UpdateExpression="set notified = :r",
		ExpressionAttributeValues={
			':r': 'false',
		},
		ReturnValues="UPDATED_NEW"
		)


		# Also try upload the description file
		try:
			data = open(staging_file_description_location, 'rb')
			s3.Bucket('freeform-scrape').put_object(Key=classification+ '/' + filename + '.description', Body=data)

			response = table.update_item(
			Key={
				'url_link': url,
			},
			UpdateExpression="set description = :r",
			ExpressionAttributeValues={
				':r': 'true',
			},
			ReturnValues="UPDATED_NEW"
			)
		except:
			print('description file no found')

		# Remove from local storage (change so only occurs on succesful upload)
		if os.path.isfile(staging_file_location):
			os.remove(staging_file_location)
			os.remove(staging_file_description_location)
			print('file uploaded and removed from local system\n')

		else:    ## Show an error ##
			print("Error: %s file not found" % staging_file_location)
	except Exception as e:
		print(e)

def create_full_html_file():

	test_file = open("all_links.html","w") 

	s3_url_generic = 'https://s3.console.aws.amazon.com/s3/buckets/freeform-scrape/?region=ap-southeast-2&tab=overview&prefixSearch='
	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	#get all sets
	response = table.scan(FilterExpression=Attr('uploaded').eq('true')&Attr('classification').eq('set'))
	to_notify_rows = response['Items']
	while 'LastEvaluatedKey' in response:
		response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'],FilterExpression=Attr('uploaded').eq('true')&Attr('classification').eq('set'))
		to_notify_rows.extend(response['Items'])

	sets_sorted_by_artist = sorted(to_notify_rows, key=lambda k: k['artist']) 

	print('sorted list, generating html file')
	for row in sets_sorted_by_artist:

		temp_link = s3_url_generic + 'set/' + row['filename']
		temp_row_name = row['classification'] + ' : ' + row['filename']
		html_link = "<a href='" + temp_link+ "''>" + temp_row_name + '</a><br>'
		test_file.write(html_link + '\n')

	#get all tracks
	test_file.write('<br>' + '\n')
	response = table.scan(FilterExpression=Attr('uploaded').eq('true')&Attr('classification').eq('track'))
	to_notify_rows = response['Items']
	while 'LastEvaluatedKey' in response:
		response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'],FilterExpression=Attr('uploaded').eq('true')&Attr('classification').eq('track'))
		to_notify_rows.extend(response['Items'])

	tracks_sorted_by_artist = sorted(to_notify_rows, key=lambda k: k['artist']) 

	print('adding tracks...')
	for row in tracks_sorted_by_artist:

		temp_link = s3_url_generic + 'track/' + row['filename']
		temp_row_name = row['classification'] + ' : ' + row['filename']
		html_link = "<a href='" + temp_link+ "''>" + temp_row_name + '</a><br>'
		test_file.write(html_link + '\n')

	test_file.close()





def send_notification_email(get_notified_full_list = False):

	# for the summary email
	today = date.today()
	today_ord = today.toordinal()


	email_body = "Files uploaded\n"
	email_sets = ""
	email_tracks = ""

	s3_url_generic = 'https://s3.console.aws.amazon.com/s3/buckets/freeform-scrape/?region=ap-southeast-2&tab=overview&prefixSearch='

	notified = 'false'
	# Get all the non notified ones
	if(get_notified_full_list):
		notified = 'true'

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	#refresh file name from dynamodb table
	response = table.scan(FilterExpression=Attr('notified').eq(notified))
	to_notify_rows = response['Items']

	while 'LastEvaluatedKey' in response:
		response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'],FilterExpression=Attr('notified').eq(notified))
		to_notify_rows.extend(response['Items'])

	for row in to_notify_rows:

		if (row['classification'] == 'set'):

			#urls for possible future use
			temp_link = s3_url_generic + 'set/' + row['filename']
			temp_row_name = row['classification'] + ' : ' + row['filename']
			html_link = '<a href=' + temp_link+ '>' + temp_row_name + '</a>'

			email_sets += temp_row_name + '\n'
		else:
			email_tracks += row['classification'] + ' : ' + row['filename'] + '\n'

		# Set notified to true
		response = table.update_item(
		Key={
			'url_link': row['url_link'],
		},
		UpdateExpression="set notified = :r",
		ExpressionAttributeValues={
			':r': 'true',
		},
		ReturnValues="UPDATED_NEW"
		)



	email_body += (email_sets + '\n' + email_tracks)

	S3_data = get_S3_size_data()

	est_monthly_storage_cost = round(0.022 * (float(S3_data[1]) + float(S3_data[3])) , 2)
	est_download_cost = round(0.09 * (float(S3_data[1]) + float(S3_data[3])) , 2)

	email_body += '\n'
	email_body += S3_data[0] + ' Tracks downloaded'+ '\n'
	email_body += S3_data[1] + 'GB total size' + '\n'
	email_body += '\n'

	email_body += S3_data[2] + ' Sets downloaded' + '\n'
	email_body += S3_data[3] + 'GB total size' + '\n'
	email_body += '\n'

	email_body += '$' + str(est_monthly_storage_cost) + ' Monthly storage cost' + '\n'
	email_body += '$' + str(est_download_cost) + ' Full download cost' + '\n'


	msg_client = boto3.client('sns',region_name='us-west-2')
	topic = msg_client.create_topic(Name="crypto-news-daily")
	topic_arn = topic['TopicArn']  # get its Amazon Resource Name
	mail_subject = 'Freeform-scrape:  ' + str(today)
	msg_client.publish(TopicArn=topic_arn,Message=email_body,Subject=mail_subject)


def main():
	
	try:
		to_run = sys.argv[1]
	except Exception as e:
		to_run = 'all'

	if (to_run == 'init'):
		print("init begins")
		init_artists.main()
		return

	if (to_run == 'info'):
		download_information_only()
		return

	if(to_run == 'all' or to_run == 'refresh'):
		print('refreshing')
		startTime_refresh = arrow.utcnow()
		rl.refresh_link_database()
		stopTime_refresh = arrow.utcnow()

	if (to_run == 'refresh_sc'):
		rl.quick_refresh_link_database(False,True)
		return

	if (to_run == 'refresh_yt'):
		rl.refresh_link_database(2020,True)
		return

	if (to_run == 'qrefresh_yt'):
		rl.quick_refresh_link_database(True,False)
		return

	if (to_run == 'qrefresh'):
		rl.quick_refresh_link_database(True,True)
		return

	if (to_run == 'song_info_download'):
		song_info_download()
		return

	if (to_run == 'create_links'):
		create_full_html_file()
		return

	if(to_run == 'all' or to_run == 'download'):
		print('downloading')
		startTime_download = arrow.utcnow()
		download_all_new_links()
		stopTime_download = arrow.utcnow()

	if(to_run == 'all' or to_run == 's3upload'):
		print('uploading to s3')
		startTime_upload = arrow.utcnow()
		upload_to_s3()
		send_notification_email()
		stopTime_upload = arrow.utcnow()

	# This defaults to only youtube
	if(to_run == 'newall'):
		startTime_upload = arrow.utcnow()
		download_upload_all_new_links()
		stopTime_upload = arrow.utcnow()

	if(to_run == 'newall_yt'):
		startTime_upload = arrow.utcnow()
		download_upload_all_new_links(platform_to_dl='youtube')
		stopTime_upload = arrow.utcnow()

	if(to_run == 'newall_sc'):
		startTime_upload = arrow.utcnow()
		download_upload_all_new_links(platform_to_dl='soundcloud')
		stopTime_upload = arrow.utcnow()	

	if(to_run == 'notify'):
		startTime_upload = arrow.utcnow()
		send_notification_email()
		stopTime_upload = arrow.utcnow()

	if(to_run == 'notify_all'):
		startTime_upload = arrow.utcnow()
		send_notification_email(True)
		stopTime_upload = arrow.utcnow()

	#organise_staging_area()	
	#classify_all_TBA_tracks()

	if(to_run == 'all' or to_run == 'refresh'):
		print('Completed Refresh Scripts in: {}'.format(stopTime_refresh - startTime_refresh))
	
	if(to_run == 'all' or to_run == 'download'):
		print('Completed Download Scripts in: {}'.format(stopTime_download - startTime_download))

	if(to_run == 'all' or to_run == 's3upload'):
		print('Completed S3Upload Scripts in: {}'.format(stopTime_upload - startTime_upload))

	return

if __name__ == "__main__":
	#fire.Fire()
	main()

