CREATE TABLE public.dim_country (
	code varchar(256) PRIMARY KEY,
	name varchar(256),
	region varchar(256),
	sun_region varchar(256)
);

CREATE TABLE public.score (
	score_id integer PRIMARY KEY,
	dim_country_code varchar(256) NOT NULL,
	dim_time_year integer NOT NULL,
	dim_indicator_code varchar(256) NOT NULL,
	value numeric(32,8)
);

CREATE TABLE public.dim_indicator (
	code varchar(256) PRIMARY KEY,
	name varchar(256),
	group varchar(256)
);

CREATE TABLE public.dim_time (
	year integer PRIMARY KEY,
	decade integer,
	century integer
);
