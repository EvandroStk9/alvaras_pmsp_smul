
#
# usethis::create_github_token()

# Token: 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

#
usethis::edit_r_environ()


# GITHUB_PAT= "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"

#
usethis::use_git_remote("origin", url = NULL, overwrite = TRUE)

#
usethis::use_github(protocol = "https", private = TRUE
                    # auth_token = Sys.getenv("GITHUB_PAT")
                    )

#
usethis::use_mit_license()

