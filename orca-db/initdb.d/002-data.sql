INSERT INTO client (clnt_id, clnt_name, clnt_mailto, clnt_logo) VALUES
	('demo', 'Version de démonstration', 'nclsdevelopment@gmail.com', 'https://s3-eu-west-1.amazonaws.com/orca-resources/logo_complete_small.png');
INSERT INTO departments (dept_pk, dept_id, dept_name) VALUES
	(0, 'NONE', 'Unassigned');
INSERT INTO sites (site_pk, site_name, site_dept_fk) VALUES
	(0, 'Unassigned', 0);
INSERT INTO employees (empl_pk, empl_firstname, empl_surname, empl_dob, empl_permanent, empl_gender) VALUES
	('root', 'root', 'Orca', date '1990-05-15', false, false);
INSERT INTO users (user_id, user_pwd, user_type, user_empl_fk) VALUES
	('root', md5('pwd'), 'employee', 'root');
INSERT INTO trainerprofiles (trpr_pk, trpr_id) VALUES
	(0, 'All');
INSERT INTO users_roles (user_id, usro_type, usro_level, usro_trpr_fk) VALUES
	('root', 'user', null, null),
	('root', 'access', 4, null),
	('root', 'trainer', null, 0),
	('root', 'admin', 4, null);
