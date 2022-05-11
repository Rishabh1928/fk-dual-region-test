inp = {"100mb_file":104857600, "500MB_file":524288000, "1gb_file":1073741824, "3gb_file":3221225472, "5gb_file":5368709120, "10gb_file":10737418240, "50gb_file": 53687091200, "100gb_file":107374182400}
for x in inp.keys():
    print(x+" started..")
    f = open(x,"wb")
    f.seek(inp[x]-1)
    f.write(b"\0")
    f.close()
    import os
    os.stat(x).st_size
    print("Done")