from __future__ import unicode_literals
from __future__ import print_function # Python 2/3 compatibility
import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import decimal
import os
import sys
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

import youtube_dl
from mutagen.mp3 import MP3

import init_artists
import refresh_lib as rl

base_fs_dir = '/home/daniel/Documents/freeform_scrape/'


def download_all_new_links():

	base_dir = base_fs_dir + 'staging/'

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	url_response = table.scan(FilterExpression=Attr('downloaded').eq("false")&Attr('uploaded').eq("false"))	
	urls_to_dl = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'])
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

def download_upload_all_new_links():

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
			s3upload_single_track(url_row)
		except Exception as e:
			print(e)

	return

def download_one_track(url_row):

	base_dir = base_fs_dir + 'staging/'
	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	try:
		url = url_row['url_link']
		platform = url_row['platform']
		artist = url_row['artist']
		title = url_row['title']
		print("\nattempting download of:" + url)

		# Perform the download 
		if (platform == 'youtube'):
			youtube_ydl_opts  = {
				'format': 'bestaudio/best',
				'outtmpl': base_fs_dir + 'staging/[%(uploader)s]%(title)s.%(ext)s',
				'writeinfojson': True,
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
		elif (platform == 'soundcloud'):
			soundcloud_ydl_opts = {
			'outtmpl': base_fs_dir + 'staging/[%(uploader)s]%(title)s.%(ext)s',
			}
			with youtube_dl.YoutubeDL(soundcloud_ydl_opts) as ydl:
				info_dict = ydl.extract_info(url, download=False)
				filename = ydl.prepare_filename(info_dict)
				name_only = filename[len(base_dir):].rsplit('.',1)[0]
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

		classify_single_track(url);

	except Exception as e:
		print(e)


def download_information_only():

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	#Get download items
	url_response = table.scan(FilterExpression=Attr('downloaded').eq("false"))	
	urls_to_dl = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'],FilterExpression=Attr('downloaded').eq("false"))
		urls_to_dl.extend(url_response['Items'])

	#Get track items
	url_response = table.scan(FilterExpression=Attr('classification').eq("track"))	
	tracks_downloaded = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'],FilterExpression=Attr('classification').eq("track"))
		tracks_downloaded.extend(url_response['Items'])

	#Get set items
	url_response = table.scan(FilterExpression=Attr('classification').eq("set"))	
	sets_downloaded = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'],FilterExpression=Attr('classification').eq("set"))
		sets_downloaded.extend(url_response['Items'])

	#print ("all urls to download now:")
	#for url_row in urls_to_dl:
	#		print(url_row['artist'] + ': ' + url_row['title'])
	print('Number of files to download:' + str(len(urls_to_dl)))
	print('Number of tracks downloaded:' + str(len(tracks_downloaded)))
	print('Number of sets downloaded:' + str(len(sets_downloaded)))


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


def classify_single_track(link_to_classify):

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
		print(e)

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

	artist_info = artists_response['Items'][0]
	url_row = artist_info

	print('url_row: ' + url_row)
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

		# Remove from local storage (change so only occurs on succesful upload)
		if os.path.isfile(staging_file_location):
			os.remove(staging_file_location)
			print('file uploaded and removed from local system\n')

		else:    ## Show an error ##
			print("Error: %s file not found" % staging_file_location)
	except Exception as e:
		print(e)


def send_notification_email():

	# for the summary email
	today = date.today()
	today_ord = today.toordinal()
	email_body = "Files uploaded\n"
	email_sets = ""
	email_tracks = ""

	# Get all the non notified ones

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	#refresh file name from dynamodb table
	response = table.scan(FilterExpression=Attr('notified').eq('false'))
	to_notify_rows = response['Items']

	for row in to_notify_rows:
		print(row)

		if (row['classification'] == 'set'):
			email_sets += row['classification'] + ' : ' + row['filename'] + '\n'
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



	email_body += (email_sets + email_tracks)		
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

	if(to_run == 'newall'):
		startTime_upload = arrow.utcnow()
		download_upload_all_new_links()
		stopTime_upload = arrow.utcnow()

	if(to_run == 'notify'):
		startTime_upload = arrow.utcnow()
		send_notification_email()
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
	main()

