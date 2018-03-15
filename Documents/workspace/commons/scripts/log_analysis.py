elb_codes = {}
backend_codes = {}
types = {}
no_ip = 0
sdk_versions = {}

index_elb_code = 8
index_backend_code = 9
index_client = 3


f = open('logs.log', 'r')
for l in f:
    splits = l.split(' ')
    elb_code = splits[index_elb_code]
    backend_code = splits[index_backend_code]
    client = splits[index_client]
    if elb_code != "200":
        print splits[4]
        # print l
        # if client.startswith('157.48.20.148'):
        if splits[4] == '-':
            no_ip += 1
        r_t = splits[13].split('?')[0]
        if r_t in types:
            types[r_t] += 1
        else:
            types[r_t] = 1
        params_split = splits[13].split('&')
        for param in params_split:
            if param.startswith('appId') or param.startswith('app_id'):
                sdk_ver = param.split('=')[1]
                if sdk_ver in sdk_versions:
                    sdk_versions[sdk_ver] += 1
                else:
                    sdk_versions[sdk_ver] = 1
    else:
        continue

    if elb_code in elb_codes:
        elb_codes[elb_code] += 1
    else:
        elb_codes[elb_code] = 1
    if backend_code in backend_codes:
        backend_codes[backend_code] += 1
    else:
        backend_codes[backend_code] = 1
f.close()
print "elb_codes", elb_codes
print "backend_codes", backend_codes
print "types", types
print "no ips", no_ip
print "sdk versions", sdk_versions