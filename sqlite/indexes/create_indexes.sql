-- Used for create_wgt_groups
CREATE INDEX idx_personid ON cps_harmonized_longitudinally_matched(personid);
CREATE INDEX idx_date ON cps_harmonized_longitudinally_matched(date);
CREATE INDEX idx_personid_date ON cps_harmonized_longitudinally_matched(personid, date);
CREATE INDEX idx_personid_wgt_groups ON wgt_groups(personid);
CREATE INDEX idx_date_wgt_groups ON wgt_groups(date);
CREATE INDEX idx_personid_date_wgt_groups ON wgt_groups(personid, date);

