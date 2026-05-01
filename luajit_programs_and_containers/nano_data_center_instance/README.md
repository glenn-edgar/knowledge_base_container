# nano_data_center_instance

Site-specific configuration tree. Sibling of `nano_data_center_base/`.
Empty stub at the moment — populated when an actual site deployment
needs site-tier KB rows, file-store seed scripts, or per-node site.json.

## Layout

```
nano_data_center_instance/
├── app_containers/                 (empty; first app port = ros_mission_planner_ii + thread_bridge)
├── configurations/
│   └── moon_base_alpha/            (FIRST SITE)
│       ├── kb_script/              (KB construction inputs — site-tier rows)
│       ├── file_scripts/           (file-store seed)
│       ├── master_node_data/
│       │   └── site.json           (role=master, NDC_BASE, master_addr, pg connect)
│       └── slave_node_data/
│           └── site.json           (role=slave, master_addr placeholder)
└── development/                    (gitignored)
```

## Wire-up

Consumed via `NDC_INSTANCE` env var. Not yet referenced by base
build/run scripts — add when site-tier construction stages or
volume-mounted KB loaders are wired in.

## Status

Skeleton only. The cluster currently runs entirely off
`nano_data_center_base/` defaults; site-tier separation activates
when the first instance deploys real site rows.
