INSERT INTO client (clnt_id, clnt_name, clnt_mailto, clnt_logo) VALUES
	('demo', 'Version de démonstration', 'nclsdevelopment@gmail.com', 'https://s3-eu-west-1.amazonaws.com/orca-resources/logo_complete_small.png');
INSERT INTO sites (site_pk, site_external_id, site_name) VALUES
	(0, md5('Unassigned'), 'Unassigned');
INSERT INTO employees (empl_pk, empl_firstname, empl_surname, empl_dob, empl_permanent, empl_gender) VALUES
	(0, 'root', 'Orca', date '1990-05-15', false, false);
INSERT INTO users (user_id, user_pwd, user_type, user_empl_fk) VALUES
	('root', md5('pwd'), 'employee', 0);
INSERT INTO trainerprofiles (trpr_pk, trpr_id) VALUES
	(0, 'All');
INSERT INTO users_roles (user_id, usro_type, usro_level, usro_trpr_fk) VALUES
	('root', 'user', null, null),
	('root', 'access', 4, null),
	('root', 'trainer', null, 0),
	('root', 'admin', 4, null);
INSERT INTO tags (tags_name, tags_type, tags_short) VALUES
	('Département', 's', 'DEPT');
