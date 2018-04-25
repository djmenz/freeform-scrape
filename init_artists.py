from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal

def insert_artist(artist, platform):

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_archive_artist')


	table.put_item(
	   Item={
		   'artist': artist,
		   'platform': platform,
		}
	)

def main():

	artists = [

			]


	for artist in artists:
		insert_artist(artist[0],artist[1])

	dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
	table = dynamodb.Table('music_archive_artist')

	artists = table.scan()
	
	for artist in (artists['Items']):
		print(artist['artist'] + ":" + artist['platform'] )

if __name__ == '__main__':
	main()