import flask
import requests

# AMBERFIN - Playin' around with the API
# ========================================================================
# Amberfin workflow instance info
# REST Endpoint

amberfin = flask.Blueprint("amberfin", __name__, )

@amberfin.route("/v1/workflow/<string:jobid>")
def get_workflow_instance(jobid):

	r = requests.get(requests.compat.urljoin(Settings.url_amberfin, f"WorkflowManagerService/rest/monitor/instances/{jobid}"))
	
	try:
		r.raise_for_status()
	except Exception as e:
		flask.abort(r.status_code, description=str(e))

	
	job_info = r.json()

	# Get better percentage complete
	
	if "currentActivities" in job_info.keys() and len(job_info.get("currentActivities")):
		job_info["progress"] = 0
		job_info["tasks"] = []
		for task in job_info.get("currentActivities"):
			job_info["progress"] += int(task.get("progress",0))
			job_info["tasks"].append(task.get("name",'?'))

		job_info["progress"] = job_info["progress"] / len(job_info.get("currentActivities"))

	return flask.jsonify(job_info)
	

# Amberfin all workflow IDs
# REST Endpoint
@amberfin.route("/v1/workflow")
def get_all_workflow_instance():
	
	r = requests.get(requests.compat.urljoin(Settings.url_amberfin, "WorkflowManagerService/rest/monitor/instances"))
	
	try:
		r.raise_for_status()
	except Exception as e:
		flask.abort(r.status_code, description=str(e))
	
	#print(f"Got {r.json()}")
	job_ids = [{"id": x.get("id","")} for x in r.json()]
	#for thing in job_ids:
	#	print(f"Thing ID: {thing.get('id')}")
	
	print(f"Ok: {flask.jsonify(job_ids)}")
	
	return flask.jsonify(job_ids)

# Amberfin Web monitor
# HTTP Repsonse
@amberfin.route("/")
def get_jobs():
	
	r = requests.get(requests.compat.urljoin(Settings.url_amberfin, "WorkflowManagerService/rest/monitor/instances"))
	
	
	try:
		r.raise_for_status()
	except Exception as e:
		abort(r.status_code, description=str(e))

	
#	content = []
#	for job in r.json():
#		for key in job.keys():
#			content.append(f"{key}:{job.get(key)}<br/>")
#		content.append("<p/>\n")
	
	return flask.render_template("viewer.html", title="Marry Me", shots= r.json())
	
#	return Response(
#		r.text,
#		status = r.status_code,
#		content_type=r.headers["content-type"]
#	)