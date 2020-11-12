import sqlite3, re
import os

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

from googleapiclient.http import MediaFileUpload
scopes = ["https://www.googleapis.com/auth/youtube.upload",
		  "https://www.googleapis.com/auth/youtube.force-ssl"]

conn = sqlite3.connect('transcode.sqlite')

playlists = {
	"OOPSLA": "PLyrlk8Xaylp5UkqDkIEMdtooA6Ktusc_x",
	"ECOOP 2020": "PLyrlk8Xaylp6P3MKkGw-rOO3fQMz4kbJs",
	"Dynamic Languages Symposium": "PLyrlk8Xaylp4DxJW9NBd5BRyoCtQJo3tQ",
	"GPCE 2020 - 19th International Conference on Generative Programming: Concepts & Experiences": "PLyrlk8Xaylp5DMgBJmTimx4n5FjNhcTFf",
	"SAS 2020 - 27th Static Analysis Symposium": "PLyrlk8Xaylp4Z25IZxVk8hdDg8IeX6ybe",
	"SLE (Software Language Engineering) 2020": "PLyrlk8Xaylp4ZN03zKczUghlrWrgKdH_P"
}

c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS YTUpload
			 (id INTEGER PRIMARY KEY, ingest_id INTEGER NOT NULL, ytid TEXT);''')

# Disable OAuthlib's HTTPS verification when running locally.
# *DO NOT* leave this option enabled in production.
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

api_service_name = "youtube"
api_version = "v3"
client_secrets_file = "/home/ec2-user/client_secrets.json"

# Get credentials and create an API client
flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
	client_secrets_file, scopes)
credentials = flow.run_console()
youtube = googleapiclient.discovery.build(
	api_service_name, api_version, credentials=credentials)

max_vids = 8
n_vids = 0
rows = []
for row in c.execute('''SELECT Ingests.id, Metadata.title, Metadata.description, Metadata.url, Metadata.urlinfo, 
					Metadata.paper_keywords, Metadata.social_tags, Metadata.social_handles, 
					Metadata.summary, Ingests.authors, Outputs.track, Ingests.video_file, Ingests.sub_file, Metadata.doi FROM Metadata 
			 LEFT JOIN Ingests ON Metadata.ingest_id = Ingests.id 
			 LEFT JOIN Transcodes ON Transcodes.ingest_id=Ingests.id 
			 LEFT JOIN Outputs ON Outputs.id = Transcodes.output_id
			 LEFT JOIN YTUpload ON YTUpload.ingest_id = Ingests.id WHERE YTUpload.ytid IS NULL'''):
	rows.append(row)
for (iid, title,desc,url,urlinfo,pkeyw,keyw,hndles,summ,authors,track,vid,sub,doi) in rows:
	print_urlinfo = ""
	print_url = ""
	if urlinfo:
		print_urlinfo = f"more info: {urlinfo}"
	if url and not ("scholar.google.com" in url):
		print_url = f"preprint url: {url}"
	desc = f'''
{summ}

{title}
Paper DOI: {doi} {print_url} {print_urlinfo}
Presented at {track}, part of SPLASH 2020
By {authors}'''
	keywords = re.split('[,;]', keyw)
	keywords.extend(re.split('[,;]', pkeyw))
	keywords = list(filter(None, map(lambda x:x.strip(), keywords)))
	
	playlist = playlists[track]
	print(f"uploading {n_vids} of {max_vids}")
	if n_vids >= max_vids:
		break
	if len(title) >= 100:
		title = title[0:100]
	n_vids = n_vids+1
	request = youtube.videos().insert(
		part="snippet,status",
		body={
			"snippet": {
				"categoryId": "28",
				"description": desc,
				"title": title, 
				"tags": keywords
			},
			"status": {
				"privacyStatus": "private"
			}
		},

		# TODO: For this request to work, you must replace "YOUR_FILE"
		#       with a pointer to the actual file you are uploading.
		media_body=MediaFileUpload(vid, resumable=True, chunksize=1024*1024*64)
	)
	video_response = None
	while video_response is None:
		status, video_response = request.next_chunk()
		if status:
			print(f"Uploaded {int(status.progress() * 100)}")
	print(f"check id {video_response['id']}")
	if video_response is None:
		print("Error in uploading")
		exit(1)
	request = youtube.captions().insert(
		part="snippet",
		body={
			"snippet": {
				"language": "en",
				"name": "English captions",
				"videoId": video_response['id'],
				"isDraft": False
			}
		},
		# TODO: For this request to work, you must replace "YOUR_FILE"
		#       with a pointer to the actual file you are uploading.
		media_body=MediaFileUpload(sub)
	)
	response = request.execute()

	request = youtube.playlistItems().insert(
	part="snippet",
	body={
		"snippet": {
			"playlistId": playlist,
			"position": 0,
			"resourceId": {
				"kind": "youtube#video",
				"videoId": video_response['id']
			}
		}
	})
	response = request.execute()
	c.execute('INSERT INTO YTUpload (ingest_id, ytid) VALUES (?, ?)', (iid, video_response['id']))
	conn.commit()
	print("iteration")
print(f"terminate {n_vids}")

conn.commit()
conn.close()