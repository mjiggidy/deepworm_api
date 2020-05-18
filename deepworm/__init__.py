import pathlib
import flask, flask_mysqldb
#from .db import mysql
#from .blueprints import dailies

def create_app(test_config = None):
	app = flask.Flask(__name__, instance_relative_config=True)
	app.config.from_mapping(
		SECRET_KEY = "dev"
	)

	if test_config is None:
		app.config.from_pyfile("config.py")
	else:
		app.config.from_mapping(test_config)

	# setup db
	mysql = flask_mysqldb.MySQL(app=app)

#	app.register_blueprint(blueprints.dailies.dailies)
	#app.register_blueprint(blueprints.amberfin.amberfin)
	@app.route("/v1/shows/")
	def list_shows_rest():
		
		cur = mysql.connection.cursor()
		cur.execute("SELECT hex(guid_show) as guid_show, title FROM dailies_shows ORDER BY title")
		results = cur.fetchall()
		
		return flask.jsonify(results)
		
	@app.route("/v1/shows/<string:show>")
	def list_show_details_rest(show):
		
		cur = mysql.connection.cursor()
		cur.execute("SELECT hex(guid_show) as guid_show, title FROM dailies_shows WHERE guid_show = unhex(%s) LIMIT 1", (show,))
		results = cur.fetchall()
		
		return flask.jsonify(results)
		
	@app.route("/v1/shots/<string:show>")
	def list_shots_by_show(show):

		cur = mysql.connection.cursor()	
		cur.execute("""
			SELECT
				hex(dailies_shots.guid_shot) as guid_shot,
				shot,
				frm_start,
				frm_end
			FROM dailies_shots
			LEFT JOIN dailies_metadata ON dailies_metadata.guid_shot = dailies_shots.guid_shot
			#LEFT JOIN dailies_proxies ON dailies_proxies.guid_shot = dailies_shots.guid_shot
			WHERE dailies_shots.guid_show = unhex(%s)
			ORDER BY dailies_shots.frm_start
		""", (show,))
		results = cur.fetchall()
		
		return flask.jsonify(results)

	@app.route("/v1/shots/<string:shot>/extended")
	def list_shot_exteded(shot):

		cur = mysql.connection.cursor()	
		cur.execute("""
			SELECT extended_info
			FROM dailies_metadata
			WHERE guid_shot = unhex(%s)
			LIMIT 1
		""", (shot,))
		results = cur.fetchone()
		
		if cur.rowcount:
			return results.get("extended_info")
		else:
			return flask.jsonify({})
			
	@app.route("/v1/diva/restore", methods=["POST"])
	def diva_restore_request():

		if not flask.request.is_json:
			return flask.jsonify({"error_message": "Must be a JSON request."}, 400)
		
		diva_request_succ = []
		diva_request_errs = []
		
		for shot in flask.request.json:
			print(f"Restoring {shot}")
			cur = mysql.connection.cursor()
			cur.execute("""
				SELECT
					hex(dailies_diva.guid_shot) as guid_shot,
					hex(dailies_diva.guid_show) as guid_show,
					dailies_diva.object_name as object_name,
					diva_config.category as category,
					diva_config.src_dest as src_dest
				FROM
					dailies_diva, diva_config
				WHERE
					diva_config.guid_show= dailies_diva.guid_show AND dailies_diva.guid_shot = UNHEX(%s)
			""", (shot.get("guid_shot"),))
			
			for diva_object in cur.fetchall():
	#			diva_request_ids.append(diva_object.get("object_name"))
				proc_diva_restore = subprocess.Popen([r"C:\Users\LTOtransfer\Desktop\DivaScript\Utilities\bin\DivaScript.exe",
					"restore",
					"-obj", diva_object.get("object_name"),
					"-cat", diva_object.get("category"),
					"-src", diva_object.get("src_dest")],
					text=True,
					universal_newlines=True,
					stdout=subprocess.PIPE, stderr=subprocess.PIPE)
				
				try:
					pipe_out, pipe_err = proc_diva_restore.communicate(timeout=10)
				except Exception as e:
					print("Retryin...")
					proc_diva_restore.kill()
					pipe_out, pipe_err = proc_diva_restore.communicate(timeout=10)
				
				if pipe_out.strip().isnumeric() and not pipe_err:
					diva_object.update({"req_id": pipe_out.strip()})
					diva_request_succ.append(diva_object)
					
					
			
		for diva_req in diva_request_succ:
			print(diva_req)
			thing = cur.execute("""
				INSERT INTO diva_requests(req_id, guid_shot, type, status, location)
				VALUES (
					%s,
					unhex(%s),
					%s,
					%s,
					%s
				)""", (diva_req.get("req_id"), diva_req.get("guid_shot"), "Restore","Restoring...",diva_req.get("src_dest"),))
			

	#		print(f"Shot {diva_object.get('object_name')}:\tOut: {pipe_out}\tErr:{pipe_err}")
			
		#print(diva_request_ids)
			
		return "Done"
			
		
				
			
	@app.route("/dailies/v1/metadata/ale", methods=["POST"])
	def ale_from_shots():

		#print(f"Is Json? {flask.request.is_json}")
		#print(f"Received data: {flask.request.json}")
		
		ale = upco_ale.Ale()
		for shot in flask.request.json:
			cur = mysql.connection.cursor()
			cur.execute("""
				SELECT 
					hex(dailies_shots.guid_shot) as guid_shot,
					shot,
					frm_start,
					frm_end,
					scene,
					take,
					camroll,
					extended_info,
					(SELECT object_name FROM dailies_diva WHERE dailies_diva.guid_shot = dailies_shots.guid_shot LIMIT 1) as diva_name,
					(SELECT proxy_name FROM dailies_proxies WHERE dailies_proxies.guid_shot = dailies_shots.guid_shot LIMIT 1) as proxy_name
				FROM dailies_shots 
				LEFT JOIN dailies_metadata ON dailies_metadata.guid_shot = dailies_shots.guid_shot 
				WHERE dailies_shots.guid_shot = unhex(%s)
			""",(shot.get("guid_shot"),))

			shot_data = cur.fetchone()
			
			if shot_data.get("proxy_name"): clip_name = shot_data.get("proxy_name")
			elif shot_data.get("diva_name"): clip_name = shot_data.get("diva_name")
			else: clip_name = shot.get("shot")
			
			shot_masterclip = {
				
				"Name": str(clip_name),
				"Tape": str(shot_data.get("shot")),
				"Start": str(upco_timecode.Timecode(shot_data.get("frm_start"))),
				"End": str(upco_timecode.Timecode(shot_data.get("frm_end"))),
				"Scene": str(shot_data.get("scene","?")),
				"Take": shot_data.get("take","?"),
				"Camroll": str(shot_data.get("camroll","?"))
			}
			
			# Unpack and merge extended_info
			if shot_data.get("extended_info"): shot_masterclip.update(flask.json.loads(shot_data.get("extended_info")))
			
			# Remove problematic columns (Color, Tracks, etc) that confuse Avid if not present for all shots
			removed = [shot_masterclip.pop(x) for x in ("Color","Tracks","Creation Date","Duration","Video","Videoframerate") if x in shot_masterclip.keys()]
			
			print(f"Removed {removed}")
			
			ale.addClips([shot_masterclip])
			
		ale_data = str()
		
		# Current upco_ale library only writes to a file.  My lack of foresight continues to astonish.
		# For now, write the ALE to a temp file, then read the file back into memory.
		with tempfile.TemporaryDirectory() as dir_temp:
			
			path_ale = pathlib.Path(dir_temp, "export.ale")
			ale.writeAle(path_ale)
			with path_ale.open("r", newline="\r\n", encoding="utf-8") as exported_ale:
				ale_data = str().join(exported_ale.readlines())
		
		return str(ale_data)

	# Create instance path
	try:
		pathlib.Path(app.instance_path).mkdir(parents=True, exist_ok=True)
	except OSError:
		pass

	return app