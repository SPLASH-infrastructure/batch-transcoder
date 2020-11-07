# batch-transcoder

Batch-transcoder is a simple transcoder designed to support our conference publishing based video ingest chain. It will

* Track already transcoded videos (by date of accession)
* Transcode videos into stream-compatible format (using ffmpeg)
* and associate them with the conf.researchr IDs based on the conference publishing metadata

In addition, it will support a "sidechain" import that allows specific videos to be loaded automatically.

# Architecture

`batch-transcoder` is a simple Julia application backed by a SQLite database, which is used to store metadata about already processed videos in a durable way. 
When triggered (externally, by a cronjob), it will scan the ingest directory (configured in the DB), identify which files have been updated compared to their
transcoded versions (as logged in the database) and kick off the transcode jobs for those files. This `default` operation will be invoked if no arguments are passed.

To name the output files, `batch-transcoder` will ingest a conference publishing format XML file as well as a researchr XML file and attempt to automatically associate them. If 
successful, it will store the result in its database. When it goes to transcode a video, it will use this mapping to identify what the output filename is using `event_id`s from
researchr. Importing the metadata will work using the `metadata` command, as
```
  batch_transcoder metadata [conference publishing] [ingest directory] [researchr] [output directory]
```

Sidechain imports are registered into the database directly, using a command line
```
  batch_transcoder sidechain [video_file].mp4 [srt_file].srt [metadata].json
```

The metadata file will use the same format as the conference publishing JSON for conformity. Notably, however, to avoid ambiguity, the metadata must have an **additional** field:
the researchr `event_id` that it is to be associated with.

`batch_transcoder` will store its database containing metadata as well as video details in the directory it is invoked from.

# Schema

Trancodes links Ingests and Outputs. Ingests, created from the conference publishing XML data, describes all of the videos we expect to have on hand. Outputs, created 
from conf.researchr data, describes all of the videos that we output. Transcodes links them, and is created by the pairing process upon ingestion of the XMLs. A warning
will be produced if there are any unmapped events upon ingestion of the XML sources, and another warning will be produced if there are any missing transcodes.

In normal operation, the program will loop through transcodes, checking the input file's (as described in Ingest) file altered time against the `transcode_date` stored
in Transcodes. If the altered time is greater than `transcode_date`, or if `transcode_date` is null, then a transcode will be queued for execution. Once all transcodes have
been identified, the transcodes are executed; upon the conclusion of each transcode, the `transcode_date` is updated.

If, at the end of the transcode operation, there are any transcodes with NULL transcode_date values, then a warning is produced for each.

Input and output paths are relative to the configuration fields `input_dir` and `output_dir`, respectively.

## Transcodes
| Column | Type | Description |
| - | - | - |
| id | INTEGER PRIMARY KEY | | 
| ingest_id | INTEGER | the ingest id |
| output_id | INTEGER | the output id |
| transcode_date | NULLABLE(INTEGER) | the UNIX timestamp of the file's last transcode |

## Ingests
| Column | Type | Description |
| - | - | - |
| id | INTEGER PRIMARY KEY | | 
| title | TEXT | the title of the talk |
| video_file | TEXT | the path to the video file to ingest | 
| sub_file | TEXT| the path to the subtitles to ingest |
| metadata_file | TEXT | the path to the metadata file for the video |

## Outputs
| Column | Type | Description |
| - | - | - |
| id | INTEGER PRIMARY KEY | | 
| title | TEXT | the title of the talk |
| event_id | TEXT | the researchr UUID for the event |

## Configuration
| Column | Type | Description |
| - | - | - |
| id | INTEGER PRIMARY KEY | | 
| key | TEXT | the name of the configuration option |
| value | TEXT | the value of the configuration option |
