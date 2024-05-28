context("Installed")

test_that("cshapes", {
  expect_true("cshapes" %in% installed.packages())
})