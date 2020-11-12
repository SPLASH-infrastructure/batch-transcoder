using SQLite, LightXML, JSON, CSV, DataFrames, Dates

# load the SQLite DB. eventually the name will be configurable
db = SQLite.DB("transcode.sqlite")
# defines the tables in SQLite. See readme for details.
function make_tables() 
	tables = """
	CREATE TABLE IF NOT EXISTS Transcodes (
	id INTEGER PRIMARY KEY,
	ingest_id INTEGER NOT NULL,
	output_id INTEGER NOT NULL,
	transcode_date INTEGER,
	UNIQUE(ingest_id, output_id)
	);"""
	ingests = """
	CREATE TABLE IF NOT EXISTS Ingests (
	id INTEGER PRIMARY KEY,
	transcode_id INTEGER,
	title TEXT NOT NULL,
	authors TEXT,
	event TEXT NOT NULL,
	video_file TEXT NOT NULL,
	sub_file TEXT NOT NULL,
	metadata_file TEXT NOT NULL,
	UNIQUE(video_file)
	);"""
	outputs = """
	CREATE TABLE IF NOT EXISTS Outputs (
	id INTEGER PRIMARY KEY,
	transcode_id INTEGER,
	title TEXT NOT NULL,
	authors TEXT NOT NULL,
	track TEXT NOT NULL,
	room TEXT NOT NULL,
	event_id TEXT NOT NULL,
	base_path TEXT NOT NULL,
	UNIQUE(event_id)
	);
	"""
	metadata = """
	CREATE TABLE IF NOT EXISTS Metadata (
	id INTEGER PRIMARY KEY,
	ingest_id INTEGER,
	description TEXT,
	social_tags TEXT,
	social_handles TEXT,
	summary TEXT,
	preprint TEXT,
	doi TEXT
	);
	"""
	DBInterface.execute(db, tables)
	DBInterface.execute(db, ingests)
	DBInterface.execute(db, outputs)
	DBInterface.execute(db, metadata)
end
make_tables()

function load_conf_pub_dir(db, dir, ingest_dir)
	modified = (0,0)
	for file in readdir(dir)
		extension = last(split(file, "."))
		if extension == "xml"
			modified = modified .+ load_conf_pub(db, joinpath(dir, file), ingest_dir)
		end
	end
	return modified
end

function load_manual_csv(db, csv)
	function check_file_exists(file, ftyp) 
		if !Base.Filesystem.ispath(file)
			println("$ftyp file does not exist at $file")
			return false
		end
		return true
	end
	data = CSV.File(csv) |> DataFrame
	insert_ingest = SQLite.Stmt(db, "INSERT OR REPLACE INTO Ingests (title, authors, event, video_file, sub_file, metadata_file) VALUES (:title, :authors, :event, :video, :subs, :metadata)")
	lookup_inserted = SQLite.Stmt(db, "SELECT id FROM Ingests WHERE title = :title")
	transcode_exists = SQLite.Stmt(db, "SELECT id FROM Transcodes WHERE ingest_id=:iid AND output_id = :oid")
	insert_transcode = SQLite.Stmt(db, "INSERT OR IGNORE INTO Transcodes (ingest_id, output_id) VALUES (:iid, :oid)")
	lookup_binding = SQLite.Stmt(db, "SELECT id FROM Outputs WHERE event_id = :eid")
	for (eid, title, track, authors, video_file, sub_file, metadata_file) in eachrow(data) 
		if ismissing(video_file) || isempty(strip(video_file))
			println("event $title does not have a video file, continuing")
			continue
		end
		if (!check_file_exists(sub_file, "subtitle") || !check_file_exists(metadata_file, "metadata"))
			continue
		end
		res = DataFrame(DBInterface.execute(lookup_binding, (eid=eid, )))
		if isempty(res)
			println("Invalid event id $eid, continuing")
			continue
		end
		output_id = first(res["id"])
		if isempty(DBInterface.execute(lookup_inserted, (title=title, )))
			DBInterface.execute(insert_ingest, (title=title, authors=authors, event=track, video=video_file, subs=sub_file, metadata=metadata_file))
		end
		inserted = DataFrame(DBInterface.execute(lookup_inserted, (title=title, )))
		ingest_id = first(inserted["id"])
		if isempty(DBInterface.execute(transcode_exists, (iid=ingest_id, oid=output_id)))
			DBInterface.execute(insert_transcode, (iid=ingest_id, oid=output_id))
		end
	end
end

function load_conf_pub(db, pub_xml, ingest_dir)
	existsq = SQLite.Stmt(db, "SELECT 1 FROM Ingests WHERE video_file = :video AND event = :event")
	insert = SQLite.Stmt(db, "INSERT OR IGNORE INTO Ingests (title, authors, event, video_file, sub_file, metadata_file) VALUES (:title, :authors, :event, :video, :subs, :metadata)")
	xdoc = nothing
	try
		xdoc = parse_file(pub_xml)
	catch e 
		error("Could not parse conference publishing XML file")
		rethrow(e)
	end
	DBInterface.execute(db, "BEGIN TRANSACTION;")
	inserted = 0
	skipped = 0
	try
		function add_definition(article, event_name)
			confpub_id = attribute(article, "id")
			title = content(first(article["title"]))
			authors = join(map(author_xml -> "$(content(first(author_xml["givennames"]))) $(content(first(author_xml["surname"])))", first(article["authors"])["author"]), ", ")
			video_file = "$ingest_dir/$confpub_id-Video.mp4"
			sub_file = "$ingest_dir/$confpub_id-Video.srt"
			metadata_file = "$ingest_dir/$confpub_id-Video.json"
			if isempty(DBInterface.execute(existsq, (video=video_file, event=event_name)))
				DBInterface.execute(insert, (title=title, authors = authors, event=event_name, video=video_file, subs=sub_file, metadata=metadata_file))
				inserted += 1
			else
				println(article)
				skipped += 1
			end
		end
		xroot = root(xdoc)
		event_name = content(first(xroot["eventmain"]))
		for eventsub in xroot["eventsub"]
			for session in eventsub["session"]
				for article in session["article"]
					add_definition(article, event_name)
				end
			end
			for track in eventsub["track"]
				for article in track["article"]
					add_definition(article, event_name)
				end
			end
		end
		DBInterface.execute(db, "COMMIT;")
	catch e
		DBInterface.execute(db, "ROLLBACK;")
		rethrow(e)
	end
	return inserted, skipped
end

function process_scheduler(db, required, base_path)
	data = JSON.parsefile(required)
	existsq = SQLite.Stmt(db, "SELECT 1 FROM Outputs WHERE event_id = :eid")
	insert = SQLite.Stmt(db, "INSERT OR IGNORE INTO Outputs (title, track, authors, room, event_id, base_path) VALUES (:title, :track, :authors, :room, :eventid, :basepath)")
	DBInterface.execute(db, "BEGIN TRANSACTION;")
	inserted = 0
	skipped = 0
	try 
		for req in data
			if isempty(DBInterface.execute(existsq, (eid=req["id"], )))
				DBInterface.execute(insert, (title=req["title"], track=req["track"], room=req["room"], authors=req["authors"], eventid=req["id"], basepath=base_path))
				inserted += 1
			else
				skipped += 1
			end
		end
		DBInterface.execute(db, "COMMIT;")
	catch e
		DBInterface.execute(db, "ROLLBACK;")
		rethrow(e)
	end
	return inserted, skipped
end

function load_metadata(db, pub_xml, ingest_dir)
	check = SQLite.Stmt(db, "SELECT id FROM Metadata WHERE ingest_id = :iid")
	lookup_id = SQLite.Stmt(db, "SELECT id FROM Ingests WHERE title = :title")
	insert = SQLite.Stmt(db, """
		INSERT INTO Metadata (ingest_id, title, abstract, url, urlinfo, event, paper_keywords, description, social_tags, social_handles, summary, doi) 
		VALUES (:iid, :title, :abst, :url, :urlinfo, :event, :paper_keywords, :description, :social_tags, :social_handles, :summary, :doi)""")
	update = SQLite.Stmt(db, """
		UPDATE Metadata SET 
			abstract=:abst, 
			url=:url, urlinfo=:urlinfo, event=:event, 
			paper_keywords=:paper_keywords, description=:description, social_tags=:social_tags, 
			social_handles=:social_handles, summary=:summary, doi=:doi
		WHERE ingest_id = :iid""")
	xdoc = nothing
	try
		xdoc = parse_file(pub_xml)
	catch e 
		error("Could not parse conference publishing XML file")
		rethrow(e)
	end
	function add_definition(article, event_name)
		confpub_id = attribute(article, "id")
		if !Base.Filesystem.ispath(joinpath(ingest_dir, "$confpub_id-Video.json"))
			return
		end
		title = content(first(article["title"]))
		abstr = content(first(article["abstract"]))
		url = content(first(article["url"]))
		urlinfo = content(first(article["urlinfo"]))
		doi = content(first(article["doi"]))
		paper_keywords = content(first(article["keywords"]))
		
		metadata_file = joinpath(ingest_dir, "$confpub_id-Video.json")
		metadata_json = JSON.parsefile(metadata_file)
		desc = metadata_json["submissionmaterialdesc"]
		tags = if haskey(metadata_json, "submissionmaterialsocialmediatags") metadata_json["submissionmaterialsocialmediatags"] else "" end
		handles = if haskey(metadata_json, "submissionmaterialsocialmediahandles") metadata_json["submissionmaterialsocialmediahandles"] else "" end
		summary = if haskey(metadata_json, "submissionmaterialtwittersummary") metadata_json["submissionmaterialtwittersummary"] else "" end

		(igid, ) = first(DBInterface.execute(lookup_id, (title=title, )))
		if isempty(DBInterface.execute(check, (iid=igid, )))
			DBInterface.execute(insert, (iid=igid,title=title,abst=abstr,url=url,urlinfo=urlinfo,event=event_name,paper_keywords=paper_keywords,description=desc,social_tags=tags,social_handles=handles,summary=summary, doi=doi))
		else
			DBInterface.execute(update, (iid=igid,title=title,abst=abstr,url=url,urlinfo=urlinfo,event=event_name,paper_keywords=paper_keywords,description=desc,social_tags=tags,social_handles=handles,summary=summary, doi=doi))
		end
	end
	xroot = root(xdoc)
	event_name = content(first(xroot["eventmain"]))
	for eventsub in xroot["eventsub"]
		for session in eventsub["session"]
			for article in session["article"]
				add_definition(article, event_name)
			end
		end
		for track in eventsub["track"]
			for article in track["article"]
				add_definition(article, event_name)
			end
		end
	end
end

function csv_ingest(db, csv)
	df = CSV.File(csv) |> DataFrame
	return df
end

function title_bind(db)
	add_mappingsl = SQLite.Stmt(db, """
	   INSERT INTO Transcodes (output_id, ingest_id) SELECT Outputs.id, Ingests.id FROM Outputs 
       LEFT JOIN Ingests ON lower(Outputs.title) LIKE "%" || lower(Ingests.title) || "%"  
       LEFT JOIN Transcodes ON Outputs.id = Transcodes.output_id
       WHERE Transcodes.id IS NULL AND Ingests.id NOT NULL""")
	DBInterface.execute(add_mappingsl)
	add_mappingsr = SQLite.Stmt(db, """
	   INSERT INTO Transcodes (output_id, ingest_id) SELECT Outputs.id, Ingests.id FROM Outputs 
       LEFT JOIN Ingests ON lower(Ingests.title) LIKE "%" || lower(Outputs.title) || "%"  
       LEFT JOIN Transcodes ON Outputs.id = Transcodes.output_id
       WHERE Transcodes.id IS NULL AND Ingests.id NOT NULL""")
	DBInterface.execute(add_mappingsr)
	authors_match = SQLite.Stmt(db, """
	INSERT INTO Transcodes (output_id, ingest_id) SELECT oid, iid FROM (SELECT Outputs.id as oid, Ingests.id as iid, COUNT(DISTINCT(Ingests.id)) AS count FROM Outputs 
      LEFT JOIN Transcodes ON Transcodes.output_id = Outputs.id
      LEFT JOIN Ingests ON Ingests.authors LIKE Outputs.authors
      WHERE Ingests.id IS NOT NULL
      GROUP BY Ingests.id) LEFT JOIN Transcodes ON Transcodes.output_id = oid 
      WHERE Transcodes.id IS NULL AND count = 1
	""")
	DBInterface.execute(authors_match)
end

function bind_events(db, title_output, title_ingest)
	find_output = SQLite.Stmt(db, "SELECT id FROM Outputs where Outputs.title = :title")
	find_ingest = SQLite.Stmt(db, "SELECT id FROM Ingests where Ingests.title = :title")
	insert = SQLite.Stmt(db, "INSERT INTO Transcodes (output_id, ingest_id) VALUES (:output_id, :ingest_id)")
	out = DataFrame(DBInterface.execute(find_output, (title=title_output, )))
	if isempty(out)
		throw("Could not find output $(title_output)")
	end

	ing = DataFrame(DBInterface.execute(find_ingest, (title=title_ingest, )))
	if isempty(ing)
		throw("Could not find input $(title_ingest)")
	end

	DBInterface.execute(insert, (output_id=first(out["id"]), ingest_id=first(ing["id"])))
end

function get_fps(file)
	ffdata = read(`ffprobe -loglevel 8 -print_format json -show_streams $file`, String) 
	data = JSON.parse(ffdata)
	for stream in data["streams"]
		if !haskey(stream, "codec_type") || stream["codec_type"] != "video"
			continue
		end
		rfr = stream["avg_frame_rate"]
		num,denom = parse.(Int, split(rfr, "/"))
		return num/denom
	end
end

function get_duration(file)
	ffdata = read(`ffprobe -loglevel 8 -print_format json -show_streams $file`, String) 
	data = JSON.parse(ffdata)
	for stream in data["streams"]
		if !haskey(stream, "codec_type") || stream["codec_type"] != "video"
			continue
		end
		rfr = stream["duration"]
		return parse.(Float64, rfr)
	end
end

function normalization_firstpass(video)
	normalize_task = `ffmpeg -y -i $video -max_muxing_queue_size 400 -af loudnorm=I=-15:LRA=9:tp=-1:print_format=json -f null -`
	out = Pipe()
	err = Pipe()
	process = run(pipeline(normalize_task, stdout=out, stderr=err))
	close(out.in)
	close(err.in)
	read_data = String(read(err))
	jsondata = join(split(read_data, "\n")[end-12:end], "\n")
	loudness_data = JSON.parse(jsondata)
	return loudness_data
end

function process_outstanding_videos(db)
	# build the workqueue
	transcodes = DataFrame(DBInterface.execute(db, """
		SELECT video_file, sub_file, metadata_file, event_id, base_path, transcode_date, ingest_id, output_id from "Transcodes" 
		INNER JOIN Ingests ON Ingests.id = Transcodes.ingest_id
		INNER JOIN Outputs ON Outputs.id = Transcodes.output_id"""))
	done_stmt = SQLite.Stmt(db, "UPDATE Transcodes SET transcode_date=:date WHERE output_id=:oid AND ingest_id=:iid")
	for (inp_vid, inp_sub, inp_metadata, eventid, base_path, transcode_date, iid, oid) in eachrow(transcodes)
		output_video = joinpath(base_path, "$eventid.mp4")
		if !Base.Filesystem.ispath(base_path)
			Base.Filesystem.mkpath(base_path)
		end
		if !Base.Filesystem.ispath(inp_vid)
			println("Missing input video $inp_vid, continuing.")
			continue
		end
		inp_mod = Base.Filesystem.mtime(inp_vid)
		if !ismissing(transcode_date) && inp_mod <= transcode_date
			continue
		end
		computed_fps = get_fps(inp_vid)
		fps_cmd = ``
		if computed_fps < 23 || computed_fps > 35
			computed_fps = 30
			fps_cmd = `-r 30`
		end
		gop_size = floor(Int, computed_fps*2)
		loudness_data = normalization_firstpass(inp_vid)

		transcode_task = `ffmpeg -y -i $inp_vid $fps_cmd -vf scale=1920:1080 -pix_fmt yuv420p -threads 0 -vcodec libx264 -g $gop_size -sc_threshold 0 -b:v 3000k -max_muxing_queue_size 400
								-bufsize 1216k -maxrate 6000k -preset medium -profile:v high -tune film 
								-acodec aac -b:a 128k -ac 2 -ar 44100 
								-af "loudnorm=I=-15:LRA=9:tp=-1:measured_I=$(loudness_data["input_i"]):measured_LRA=$(loudness_data["input_lra"]):measured_tp=$(loudness_data["input_tp"]):offset=$(loudness_data["target_offset"]),aresample=async=1:min_hard_comp=0.100000:first_pts=0" $output_video`
		proc = run(transcode_task)
		wait(proc)
		if proc.exitcode != 0
			println("Errored while processing video $inp_vid with ID $eventid")
			continue
		end
		DBInterface.execute(done_stmt, (date=floor(Int, datetime2unix(Dates.now())), oid=oid, iid=iid))
	end
end

function reset_transcodes(db)
	DBInterface.execute(db, "UPDATE Transcodes SET transcode_date=NULL")
end

function copy_metadata(db)
	# build the workqueue
	transcodes = DataFrame(DBInterface.execute(db, """
		SELECT video_file, sub_file, metadata_file, event_id, base_path, transcode_date, ingest_id, output_id from "Transcodes" 
		INNER JOIN Ingests ON Ingests.id = Transcodes.ingest_id
		INNER JOIN Outputs ON Outputs.id = Transcodes.output_id"""))
	for (inp_vid, inp_sub, inp_metadata, eventid, base_path, transcode_date, iid, oid) in eachrow(transcodes)
		output_subs = joinpath(base_path, "$eventid.srt")
		output_meta = joinpath(base_path, "$eventid.json")
		if !Base.Filesystem.ispath(inp_sub)
			println("Missing $inp_sub")
			continue
		end
		if !Base.Filesystem.ispath(inp_metadata)
			println("Missing $inp_metadata")
			continue
		end
		Base.Filesystem.cp(inp_sub, output_subs, force=true)
		Base.Filesystem.cp(inp_metadata, output_meta, force=true)
	end
end

function check_conference(db)
	#first, identify outputs that haven't been matched to inputs
	unmatched = DataFrame(DBInterface.execute(db, """
		SELECT Outputs.title, Outputs.authors, Outputs.track, Outputs.room from Outputs 
		LEFT JOIN Transcodes ON Transcodes.output_id = Outputs.id
		WHERE Transcodes.id IS NULL"""))
	if !isempty(unmatched)
		println("Unmatched required outputs!")
		for (title, authors, track, room) in eachrow(unmatched)
			println("Unmatched: $title by $authors at $track in $room")
		end
	end
	# next, identify inputs which are mapped to outputs that don't correspond to a video 
	required_inp_files = DataFrame(DBInterface.execute(db, """
		SELECT Ingests.title, Ingests.event, Ingests.video_file, Ingests.sub_file, Ingests.metadata_file from Ingests 
		INNER JOIN Transcodes ON Transcodes.ingest_id = Ingests.id"""))
	for (title, event, vid, sub, meta) in eachrow(required_inp_files)
		missing_els = []
		hasmissing = false
		if !Base.Filesystem.ispath(vid)
			push!(missing_els, "video")
			hasmissing = true
		end
		if !Base.Filesystem.ispath(sub)
			push!(missing_els, "subs")
			hasmissing = true
		end
		if !Base.Filesystem.ispath(meta)
			push!(missing_els, "meta")
			hasmissing = true
		end
		if hasmissing
			println("Missing $(join(missing_els, ", ")) for $title at $event w/ name $vid")
		end
	end
	# check to see if there are any outstanding transcodes
	transcodes_to_do = DataFrame(DBInterface.execute(db, """
		SELECT Transcodes.transcode_date, Ingests.video_file from Transcodes 
		INNER JOIN Ingests ON Transcodes.ingest_id = Ingests.id"""))
	for (tcd, ingest_file) in eachrow(transcodes_to_do)
		if Base.Filesystem.ispath(ingest_file) && (ismissing(tcd) || tcd < Base.Filesystem.mtime(ingest_file))
			println("Need to do transcode $ingest_file")
		end
	end
end

using DataFramesMeta

function schedule_status(db)
	events = DataFrame(DBInterface.execute(db, """
		SELECT Outputs.title, Outputs.event_id, Outputs.base_path, Transcodes.id FROM Outputs
		LEFT JOIN Transcodes ON Transcodes.output_id = Outputs.id"""))
	file_exists(base_path, id) = Base.Filesystem.ispath(joinpath(base_path, "$id.mp4"))
	get_duration_l(base_path, id) = if file_exists(base_path, id) get_duration(joinpath(base_path, "$id.mp4")) else -1 end
	CSV.write("status.csv", @linq events |> transform(has_file=file_exists.(:base_path, :event_id), (duration=get_duration_l.(:base_path, :event_id))))
end

function write_sv_csv(db)
	tovalidate = DataFrame(DBInterface.execute(db, """
		SELECT Outputs.title, Outputs.authors, Outputs.track, Outputs.event_id, Outputs.base_path, Transcodes.id FROM Outputs
		LEFT JOIN Transcodes ON Transcodes.output_id = Outputs.id"""))
	make_path(base_path, id) = return "rtmp://3.18.146.46/vod/mp4:store/$base_path/$id.mp4"
	file_exists(base_path, id) = Base.Filesystem.ispath(joinpath(base_path, "$id.mp4"))
	towrite = @linq tovalidate |> transform(url=make_path.(:base_path, :event_id)) |> where(file_exists.(:base_path, :event_id))
	CSV.write("sv_csv.csv", towrite)
end

#=
if length(ARGS) == 0
	action = "default"
else
	action = ARGS[1]
end

if action == "metadata"
	process_metadata(ARGS[2], ARGS[3], ARGS[4], ARGS[5])
elseif action == "sidechain"
	add_sidechain(ARGS[2], ARGS[3], ARGS[4])
else
	error("Invalid action $action; valid values are metadata and sidechain")
end
=#