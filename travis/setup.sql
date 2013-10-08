CREATE ROLE pass_user PASSWORD 'password' LOGIN;
CREATE ROLE md5_user PASSWORD 'password' LOGIN;
CREATE DATABASE pass_user OWNER pass_user;
CREATE DATABASE md5_user OWNER md5_user;
