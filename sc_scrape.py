from __future__ import unicode_literals
from __future__ import print_function # Python 2/3 compatibility
import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import decimal
import os
import sys
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

def get_artists_to_download():

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_archive_artist')
	artists_response = table.scan()
	artists = artists_response['Items']
	return artists

def refresh_link_database():

	artist_list = get_artists_to_download()

	youtube_artists = []
	soundcloud_artists = []

	for artist_row in artist_list:
		if (artist_row['platform'] == 'soundcloud'):
			soundcloud_artists.append(artist_row['artist'])
		elif (artist_row['platform'] == 'youtube'):
			youtube_artists.append(artist_row['artist'])
		else:
			print ('invalid platform in database')

	print('---Soundcloud')	
	for artist in soundcloud_artists:
		sc_refresh_link_database_for_artist(artist)
		print('Completed: ' + artist)

	print('---Youtube')
	for artist in youtube_artists:
		yt_refresh_link_database_for_artist(artist)
		print('Have not Completed: ' + artist)

	return

def sc_refresh_link_database_for_artist(artist_to_dl):
	non_mixes = ['reposts','likes','albums','sets','tracks','following','followers']
	options = Options()
	options.add_argument('-headless')
	#driver = webdriver.Firefox()
	driver = Firefox(executable_path='geckodriver', firefox_options=options)
	url1 = 'https://soundcloud.com/' + artist_to_dl + '/tracks'
	driver.get(url1)

	#autoscroll
	pause = 2
	last_height = driver.execute_script("return document.body.scrollHeight")

	for zz in range(0,100):

		driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
		time.sleep(pause)
		new_height = driver.execute_script("return document.body.scrollHeight")
		if new_height == last_height:
			break
		last_height = new_height


	html_doc = ""
	html_doc = driver.page_source.encode('utf-8')

	#print(html_doc)

	soup = BeautifulSoup(html_doc, 'html.parser')
	#print(soup)

	link_frags = set()

	for link in soup.find_all('a'):
		if str(link).find('/' + artist_to_dl + '/') > 0:
			if not (str(link.get('href')).endswith('/comments')):
				print(link.get('href'))
				if (str(link.get('href')).split('/')[2]) not in non_mixes:
					link_frags.add(link.get('href'))


	links_full = []
	for link in link_frags:
		if (link[0:4] != 'http'):
			full_link = 'https://soundcloud.com' + str(link)
			#print(full_link)
			links_full.append(full_link)

	#print(len(links_full))
	
	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	print('--- refreshing links in db')
	for url in links_full:
		try:
			table.put_item(
				Item={
					'url_link': url,
					'platform': 'soundcloud',
					'artist': artist_to_dl,
					'downloaded': 'false',
				},
				ConditionExpression='attribute_not_exists(url_link)'
			)
		except Exception as e:
			print('already in database')
			continue
	return

def yt_refresh_link_database_for_artist_slow(artist_to_dl):

	ydl = youtube_dl.YoutubeDL({'outtmpl': '%(id)s%(ext)s', 'quiet':True,})
	video = ""
	yt_url = 'https://www.youtube.com/user/'+ artist_to_dl
	links_full = []
	yt_titles = []

	with ydl:
		result = ydl.extract_info \
		(yt_url,
		download=False) #We just want to extract the info

		if 'entries' in result:
			# Can be a playlist or a list of videos
			video = result['entries']

			#loops entries to grab each video_url
			for i, item in enumerate(video):
				video = result['entries'][i]['webpage_url'] 
				yt_title = result['entries'][i]['title']
				links_full.append([video,yt_title])
				print(yt_title)

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	for url in links_full:
		try:
			print(url[0])
			table.put_item(
				Item={
					'url_link': url[0],
					'title' : url[1],
					'platform': 'youtube-2',
					'artist': artist_to_dl,
					'downloaded': 'false',
				},
				ConditionExpression='attribute_not_exists(url_link)'
			)
		except Exception as e:
			print('already in database')
			continue

	return


def yt_artist_to_channel_id(artist_to_dl):
	
	# Get youtube api key
	youtube_api_file = open("youtube_api_key","r")
	youtube_api_key = youtube_api_file.readline()

	url = 'https://www.googleapis.com/youtube/v3/channels?key={}&forUsername={}&part=id'.format(youtube_api_key, artist_to_dl)
	inp = urllib.request.urlopen(url)
	resp = json.load(inp)
	channel_id = (resp['items'][0]['id'])
	
	return channel_id

def yt_refresh_link_database_for_artist(artist_to_dl):

	channel_id = yt_artist_to_channel_id(artist_to_dl)
	
	# Get youtube api key
	youtube_api_file = open("youtube_api_key","r")
	youtube_api_key = youtube_api_file.readline()

	api_key = youtube_api_key

	base_video_url = 'https://www.youtube.com/watch?v='
	base_search_url = 'https://www.googleapis.com/youtube/v3/search?'

	first_url = base_search_url+'key={}&channelId={}&part=snippet,id&order=date&maxResults=50'.format(api_key, channel_id)

	links_full = []
	url = first_url

	while True:
		inp = urllib.request.urlopen(url)
		resp = json.load(inp)

		for i in resp['items']:
			if i['id']['kind'] == "youtube#video":
				temp_url = (base_video_url + i['id']['videoId'])
				links_full.append([temp_url, i['snippet']['title']])
		try:
			next_page_token = resp['nextPageToken']
			url = first_url + '&pageToken={}'.format(next_page_token)
		except:

			break

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	for url in links_full:
		try:
			print(url[0])
			table.put_item(
				Item={
					'url_link': url[0],
					'title' : url[1],
					'platform': 'youtube',
					'artist': artist_to_dl,
					'downloaded': 'false',
				},
				ConditionExpression='attribute_not_exists(url_link)'
			)
		except Exception as e:
			print('already in database')
			continue	

	return



def download_all_new_links():

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	url_response = table.scan(FilterExpression=Attr('downloaded').eq("false"))	
	urls_to_dl = url_response['Items']

	while 'LastEvaluatedKey' in url_response:
		url_response = table.scan(ExclusiveStartKey=url_response['LastEvaluatedKey'])
		urls_to_dl.update(response['Items'])

	print ("all urls to download now")
	for url_row in urls_to_dl:
		print(url_row['url_link'])
	
	print('Number of files to download:' + str(len(urls_to_dl)))
	

	for url_row in urls_to_dl:
		url = url_row['url_link']
		platform = url_row['platform']
		artist = url_row['artist']
		print("\nattempting download of:" + url)

		# Perform the download 
		if (platform == 'youtube'):
			youtube_ydl_opts  = {
				'format': 'bestaudio/best',
				'outtmpl': '/home/daniel/Documents/freeform_scrape/staging/[%(uploader)s]%(title)s.%(ext)s',
				'postprocessors': [{
					'key': 'FFmpegExtractAudio',
					'preferredcodec': 'mp3',
					'preferredquality': '192',
				}],
			}
			with youtube_dl.YoutubeDL(youtube_ydl_opts) as ydl:
				ydl.download([url])
				print("downloaded:" + url)

				# Update the table if download was successful
				table.update_item(
						Item={
							'url_link': url,
							'platform': platform,
							'artist': artist,
							'downloaded': 'true',
						},
					)

		elif (platform == 'soundcloud'):
			soundcloud_ydl_opts = {
			'outtmpl': '/home/daniel/Documents/freeform_scrape/staging/[%(uploader)s]%(title)s.%(ext)s',
			}
			with youtube_dl.YoutubeDL(soundcloud_ydl_opts) as ydl:
				ydl.download([url])
				print("downloaded:" + url)

				# Update the table if download was successful
				table.update_item(
						Item={
							'url_link': url,
							'platform': platform,
							'artist': artist,
							'downloaded': 'true',
						},
					)

	return

def organise_staging_area():
	#check length of every file in staging area
	# if under <600 seconds, move to track, otherwise move to sets
	base_dir = '/home/daniel/Documents/freeform_scrape/'
	staging_tracks = os.listdir(base_dir + 'staging')

	for track in staging_tracks:
		staging_file_location = (base_dir + 'staging/' + track)
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

	return


def main():

	# To remove
	#yt_refresh_link_database_for_artist_2('ThePrimeThanatos')
	#yt_refresh_link_database_for_artist('ThePrimeThanatos')
	#return

	try:
		to_run = sys.argv[1]
	except Exception as e:
		to_run = 'all'

	if (to_run == 'init'):
		print("init begins")
		init_artists.main()
		return

	if(to_run == 'all' or to_run == 'refresh'):
		print('refreshing')
		startTime_refresh = arrow.utcnow()
		refresh_link_database()
		stopTime_refresh = arrow.utcnow()
	
	if(to_run == 'all' or to_run == 'download'):
		print('downloading')
		startTime_download = arrow.utcnow()
		download_all_new_links()
		stopTime_download = arrow.utcnow()
	
	organise_staging_area()

	if(to_run == 'all' or to_run == 'refresh'):
		print('Completed Refresh Scripts in: {}'.format(stopTime_refresh - startTime_refresh))
	
	if(to_run == 'all' or to_run == 'download'):
		print('Completed Download Scripts in: {}'.format(stopTime_download - startTime_download))

	return

if __name__ == "__main__":
	main()
