
CREATE INDEX advertiser_by_updated_at ON advertiser (updated_at);
CREATE INDEX advertiser_by_created_at ON advertiser (created_at);

CREATE INDEX campaign_by_updated_at ON campaign (updated_at);
CREATE INDEX campaign_by_created_at ON campaign (created_at);

CREATE INDEX impressions_by_created_at ON impressions (created_at);
CREATE INDEX clicks_by_created_at ON clicks (created_at);
