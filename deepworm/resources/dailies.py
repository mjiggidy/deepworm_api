# Actual stuff for reading/writing from database
from ..db import db

def getShots(guid_show=None, guid_shot=None):
	""" Return a list of dicitonaries for given GUIDs """

	# Assemble optional GUIDs for WHERE clause
	# TODO: I think I'm overcomplicating things here
	params = ""
	if guid_shot is not None:
		params = "WHERE s.guid_shot = uuid_to_bin(%s) LIMIT 1"
	elif guid_show is not None:
		params = "Where s.guid_show = uuid_to_bin(%s)"

	print(f"Using params: {params} with args {tuple([guid_shot or guid_show])}")

	cur = db.connection.cursor()	
	cur.execute(f"""
		SELECT
			bin_to_uuid(s.guid_shot) as guid_shot,
			s.shot as shot,
			s.frm_start as frm_start,
			s.frm_duration as frm_duration,
			s.frm_end as frm_end,
			IFNULL(m.extended_info, JSON_OBJECT()) as metadata
		FROM dailies_shots s
		LEFT JOIN dailies_metadata m ON m.guid_shot = s.guid_shot
		{params}
	""", tuple([guid_shot or guid_show]))
	
	return cur.fetchall()