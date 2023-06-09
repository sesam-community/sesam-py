# generic login flow for connectors that uses an api_key for authorization (for instance Freshteam)

def login_via_api_key(sesam_node, args):
    system_id = args.system_placeholder
    api_key = args.api_key
    base_url = args.base_url
    if base_url and system_id and api_key:
        is_failed = False
        try:
            system = sesam_node.get_system(system_id)
            system.put_secrets({"api_key": api_key})
        except Exception as e:
            is_failed = True
            sesam_node.logger.error("Failed to put secrets: %s" % e)

        if not is_failed:
            sesam_node.logger.info(
                "All secrets and environment variables have been updated successfully, "
                "now go and do your development!"
            )
        else:
            sesam_node.logger.error(
                "Failed to update all secrets and environment variables. see the log "
                "for details."
            )
    else:
        sesam_node.logger.error(
            "Missing arguments, please provide all required arguments"
        )
