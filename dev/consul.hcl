acl {
  enabled = true
  default_policy = "deny"
  enable_token_persistence = true

  tokens {
    master = "dev"
    agent  = "dev"
  }
}