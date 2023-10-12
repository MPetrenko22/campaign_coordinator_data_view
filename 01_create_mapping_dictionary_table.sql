/*Create dictionary to keep field mapping for view*/
CREATE TABLE power_bi_cc_field_mapping(
	attribute varchar(100),
	field varchar(100)
);

/*Add index to dictionary to faster view*/
CREATE INDEX IXX ON power_bi_cc_field_mapping(attribute);
