class Args:
    def __init__(self):
        # Needed to run anything
        self.whitelist_file = None
        self.extra_extra_verbose = True
        self.disable_json_html_escape = False

        # Needed to call anything SesamNode related
        self.node_url = "http://localhost/api"
        self.sesam_node = None
        self.jwt = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NDU4Mjc5MTYuODY4MTE1NCwiZXhwIjoxNzc3MzYzOTAwLCJ1c2VyX2lkIjoiNTVlN2JlZTctYTVmYy00MjE2LThkNmQtYTljNTNkNGYzOWVkIiwidXNlcl9wcm9maWxlIjp7ImVtYWlsIjoiYWRtaW5Ac2VzYW0uaW8iLCJuYW1lIjoiTm9ybWFsIEFkbWluc29uIiwicGljdHVyZSI6IiJ9LCJ1c2VyX3ByaW5jaXBhbCI6Imdyb3VwOkV2ZXJ5b25lIiwicHJpbmNpcGFscyI6eyI2ZjdmMDNlNC1hNmM3LTQyNGEtODkwMC1lOTE1MzljOGY1NGUiOlsiZ3JvdXA6QWRtaW4iXX0sImFwaV90b2tlbl9pZCI6ImVlZmExNzI5LWM5NjktNDEwMS1iMDk2LTJhYmFlZTlhNTgxYiJ9.MaajdsKB6Cge_7jIL_1pJGXHe741sb-d1u6IM8Zm4Tpb14MNgZj3V4AUuAxcQlWMSolJL0hrN_DQC80w55ak7meyG-v0opWbp5bI-rh6uCrHeY-a0tZSicrnzZSsRnHq_oKGQTWe0x9qRyLP9fdbbns7N8P41o8o8Cfoco9RC4d3nCXtWPhduFgp8yCU97rAqP9M_CbVlD9ZUwQWEXbXv-YRItLsNB-s11cMjpIhtXlnRAZGdoY_m89dBuEZgLcqsbvPMeXGQjqVCq98TkAp1sclPInKICkLZyck8BbX6An-epTVQ_B_8v9CwKkIhSzHg0WMKzK8jN5jcoPqfd_r-g"  # noqa: E501
