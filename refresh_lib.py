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


def refresh_link_database():
	artist_list = get_artists_to_download()

	youtube_artists = []
	soundcloud_artists = []
	
	for artist_row in artist_list:
		if (artist_row['platform'] == 'soundcloud'):
			try:
				soundcloud_artists.append(artist_row['artist'])
			except:
				print('error with:' + str(artist_row['artist']))
		elif (artist_row['platform'] == 'youtube'):
			try:
				youtube_artists.append(artist_row['artist'])
			except:
				print('error with:' + str(artist_row['artist']))
		else:
			print ('invalid platform in database')

	print('---Soundcloud')	
	for artist in soundcloud_artists:
		print('Refreshing: ' + artist)
		sc_refresh_link_database_for_artist(artist)
		print('Completed: ' + artist + '\n')

	print('---Youtube')
	for artist in youtube_artists:
		print('Refreshing: ' + artist)
		yt_refresh_link_database_for_artist(artist)
		print('Completed: ' + artist + '\n')
	return

def get_artists_to_download():

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_archive_artist')
	artists_response = table.scan()
	artists = artists_response['Items']
	return artists

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
		print("scrolling")

		driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
		time.sleep(pause)
		new_height = driver.execute_script("return document.body.scrollHeight")
		if new_height == last_height:
			break
		last_height = new_height


	html_doc = ""
	html_doc = driver.page_source.encode('utf-8')
	driver.close()

	#print(html_doc)

	soup = BeautifulSoup(html_doc, 'html.parser')
	#print(soup)

	link_frags = set()

	for link in soup.find_all('a'):
		if str(link).find('/' + artist_to_dl + '/') > 0:
			if not (str(link.get('href')).endswith('/comments')):
				#print(link.get('href'))
				if (str(link.get('href')).split('/')[2]) not in non_mixes:
					link_frags.add(link.get('href'))


	links_full = []
	for link in link_frags:
		if (link[0:4] != 'http'):
			full_link = 'https://soundcloud.com' + str(link)
			#print(full_link)
			links_full.append([full_link,link.split('/')[2]])

	print('Number of links: ' + str(len(links_full)))
	
	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	print('--- refreshing links in db')
	counter = 0
	for url in links_full:
		try:
			table.put_item(
				Item={
					'url_link': url[0],
					'title' : url[1],
					'platform': 'soundcloud',
					'artist': artist_to_dl,
					'downloaded': 'false',
					'uploaded' : 'false',
					'classification' : 'TBA',
				},
				ConditionExpression='attribute_not_exists(url_link)'
			)
			counter += 1
		except Exception as e:
			#print('already in database');
			continue
	print('links added: ' + str(counter))
	return

def yt_artist_to_channel_id(artist_to_dl):
	
	#Exception for Channel without username
	if (artist_to_dl == 'Odysseus'):
		return 'UCwoTj-pZgZZ8DInOXSSLMmA'
	
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


	# Searches all years between 2009 - 2021
	years_RFC3339_pairs = []
	for x in range(2009,2021):
		start_year = (str(x)+'-01-01T00:00:00Z')
		finish_year = (str(x+1)+'-01-01T00:00:00Z')
		years_RFC3339_pairs.append([start_year,finish_year])

	urls_by_date = []
	for row in years_RFC3339_pairs:
		search_start_date = row[0]
		search_finish_date = row[1]
		url_one_year = base_search_url + 'key={}&channelId={}&part=snippet,id&order=date&maxResults=50&publishedAfter={}&publishedBefore={}'.format(api_key, channel_id,search_start_date,search_finish_date)
		urls_by_date.append(url_one_year)

	links_full = []

	for url in urls_by_date:
		first_url = url
		while True:
			inp = urllib.request.urlopen(url)
			resp = json.load(inp)

			for i in resp['items']:
				if i['id']['kind'] == "youtube#video":
					if i['snippet']['liveBroadcastContent'] == 'none':
						temp_url = (base_video_url + i['id']['videoId'])
						links_full.append([temp_url, i['snippet']['title']])
			try:
				next_page_token = resp['nextPageToken']
				url = first_url + '&pageToken={}'.format(next_page_token)
			except:

				break

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_url_archive')

	print('Number of links: ' + str(len(links_full)))
	counter = 0
	for url in links_full:
		try:
			#print(url[0])
			table.put_item(
				Item={
					'url_link': url[0],
					'title' : url[1],
					'platform': 'youtube',
					'artist': artist_to_dl,
					'downloaded': 'false',
					'uploaded' : 'false',
					'classification' : 'TBA',
				},
				ConditionExpression='attribute_not_exists(url_link)'
			)
			counter += 1
		except Exception as e:
			#print('already in database')
			continue	
	print('links added: ' + str(counter))
	return

# This is for testing purposes - just refreshes the first soundcloud artist only
def main():
	artist_list = get_artists_to_download()

	soundcloud_artists = []
	
	for artist_row in artist_list:
		if (artist_row['platform'] == 'soundcloud'):
			soundcloud_artists.append(artist_row['artist'])
		else:
			print ('invalid platform in database')

	artist = soundcloud_artists[1]
	print('---Soundcloud')	
	print('Refreshing: ' + artist)
	sc_refresh_link_database_for_artist(artist)
	print('Completed: ' + artist + '\n')

	

if __name__ == "__main__":
	main()