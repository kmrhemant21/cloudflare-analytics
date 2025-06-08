CREATE TABLE IF NOT EXISTS dns_logs (
    ColoCode VARCHAR(50),
    EDNSSubnet VARCHAR(50),
    EDNSSubnetLength INT,
    QueryName VARCHAR(255),
    QueryType INT,
    ResponseCached BOOLEAN,
    ResponseCode INT,
    SourceIP VARCHAR(50),
    Timestamp VARCHAR(50),
    timestamp_parsed TIMESTAMP
);