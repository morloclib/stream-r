# R implementations of the stream module's native functions.

morloc_fold_source <- function(step, init, produce) {
  acc <- init
  produce(function(xs) {
    acc <<- step(acc, xs)
    invisible(NULL)
  })
  acc
}

# Batches are joined with c() only when non-empty, so that the class of the
# first batch (such as integer64) decides the method.
.morloc_stream_join <- function(batches) {
  batches <- Filter(function(xs) length(xs) > 0, batches)
  if (length(batches) == 0) {
    return(list())
  }
  do.call(c, unname(batches))
}

morloc_collect_source <- function(produce) {
  batches <- list()
  produce(function(xs) {
    batches[[length(batches) + 1L]] <<- xs
    invisible(NULL)
  })
  .morloc_stream_join(batches)
}

# Summaries merged in stream order as a binary counter (see stream-rust):
# adjacent results of equal rank merge, so each summary takes part in
# O(log n) merges.
morloc_reduce_native <- function(summarize, merge, pull) {
  ranks <- integer(0)
  stack <- list()
  finish <- function() {
    if (length(stack) == 0) {
      return(summarize(list()))
    }
    acc <- stack[[1]]
    for (b in stack[-1]) {
      acc <- merge(acc, b)
    }
    acc
  }
  repeat {
    r <- pull()
    if (!isTRUE(r[[1]])) {
      return(list(FALSE, r[[2]], finish()))
    }
    xs <- r[[3]]
    if (length(xs) == 0) {
      return(list(TRUE, "", finish()))
    }
    b <- summarize(xs)
    k <- 0L
    while (length(ranks) > 0 && ranks[length(ranks)] == k) {
      n <- length(ranks)
      b <- merge(stack[[n]], b)
      stack[[n]] <- NULL
      ranks <- ranks[-n]
      k <- k + 1L
    }
    stack[[length(stack) + 1L]] <- b
    ranks <- c(ranks, k)
  }
}

morloc_take_pull_native <- function(n, pull) {
  batches <- list()
  got <- 0
  while (got < n) {
    r <- pull()
    if (!isTRUE(r[[1]])) {
      return(list(FALSE, r[[2]], .morloc_stream_join(batches)))
    }
    xs <- r[[3]]
    if (length(xs) == 0) {
      break
    }
    xs <- xs[seq_len(min(length(xs), n - got))]
    batches[[length(batches) + 1L]] <- xs
    got <- got + length(xs)
  }
  list(TRUE, "", .morloc_stream_join(batches))
}


# External sorting
#
# Words are bit64 integer64 values in [0, 2^63), the range of U64 that R can
# hold, where signed and unsigned order agree. A record of `width` words is
# ordered by 2 * width double keys, the high and low 32 bits of each word.

.morloc_sorted_batch <- 65536L
.morloc_max_runs <- 64L
.morloc_sorters <- new.env()
.morloc_next_sorter <- 0L

.morloc_as_words <- function(xs) {
  if (length(xs) == 0) {
    return(bit64::integer64(0))
  }
  if (is.list(xs)) {
    xs <- do.call(c, unname(xs))
  }
  if (!bit64::is.integer64(xs)) {
    if (any(is.na(xs)) || any(xs < 0) || any(xs != trunc(xs)) || any(xs >= 2^63)) {
      stop("pushSorter: a word is not an unsigned 64-bit integer R can hold")
    }
    xs <- bit64::as.integer64(xs)
  }
  if (any(is.na(xs)) || any(xs < bit64::as.integer64(0))) {
    stop("pushSorter: a word is not an unsigned 64-bit integer R can hold")
  }
  xs
}

# The order keys of the records in `xs`, one column per half word.
.morloc_keys <- function(xs, width) {
  n <- length(xs) %/% width
  keys <- vector("list", 2L * width)
  two32 <- bit64::as.integer64(4294967296)
  for (j in seq_len(width)) {
    w <- xs[seq.int(j, by = width, length.out = n)]
    keys[[2L * j - 1L]] <- as.double(w %/% two32)
    keys[[2L * j]] <- as.double(w %% two32)
  }
  keys
}

# The words of the records of `xs` in the order `o`.
.morloc_take_records <- function(xs, width, o) {
  if (length(o) == 0) {
    return(bit64::integer64(0))
  }
  idx <- as.vector(rbind(outer(seq_len(width) - 1L, (o - 1L) * width, "+")) + 1L)
  xs[idx]
}

.morloc_sort_records <- function(xs, width) {
  if (length(xs) == 0) {
    return(xs)
  }
  .morloc_take_records(xs, width, do.call(order, .morloc_keys(xs, width)))
}

# TRUE for each record of `keys` that is at most the record `bound`.
.morloc_at_most <- function(keys, bound) {
  n <- length(keys[[1]])
  le <- rep(TRUE, n)
  undecided <- rep(TRUE, n)
  for (j in seq_along(keys)) {
    lt <- keys[[j]] < bound[[j]]
    gt <- keys[[j]] > bound[[j]]
    le[undecided & gt] <- FALSE
    undecided <- undecided & !lt & !gt
  }
  le
}

# A run file, unlinked from its directory as soon as it is created.
.morloc_write_run <- function(dir, words) {
  path <- tempfile("morloc-sort-", tmpdir = dir)
  con <- file(path, open = "w+b")
  unlink(path)
  writeBin(unclass(words), con, size = 8L)
  flush(con)
  list(con = con, words = length(words))
}

# A pull over the records of sorted runs, merged a chunk of each at a time:
# every buffered record no greater than the smallest last buffered record of
# a run that still has words is in its final place.
.morloc_merger <- function(runs, width) {
  chunk <- .morloc_sorted_batch * width
  left <- vapply(runs, function(r) r$words, numeric(1))
  for (r in runs) {
    seek(r$con, 0, rw = "read")
  }
  bufs <- lapply(runs, function(r) bit64::integer64(0))
  fill <- function(i) {
    n <- min(chunk, left[i])
    if (n > 0) {
      got <- readBin(runs[[i]]$con, "double", n = n, size = 8L)
      if (length(got) != n) {
        stop("a sort run ends early")
      }
      class(got) <- "integer64"
      bufs[[i]] <<- c(bufs[[i]], got)
      left[i] <<- left[i] - n
    }
  }
  function() {
    for (i in seq_along(runs)) {
      if (length(bufs[[i]]) == 0) fill(i)
    }
    live <- which(vapply(bufs, length, integer(1)) > 0)
    if (length(live) == 0) {
      return(bit64::integer64(0))
    }
    open <- live[left[live] > 0]
    if (length(open) == 0) {
      out <- do.call(c, unname(bufs[live]))
      bufs[live] <<- lapply(live, function(i) bit64::integer64(0))
      return(.morloc_sort_records(out, width))
    }
    lasts <- do.call(c, lapply(bufs[open], function(b) b[(length(b) - width + 1L):length(b)]))
    lkeys <- .morloc_keys(lasts, width)
    m <- do.call(order, lkeys)[1]
    bound <- lapply(lkeys, function(k) k[m])
    parts <- list()
    for (i in live) {
      b <- bufs[[i]]
      ok <- .morloc_at_most(.morloc_keys(b, width), bound)
      parts[[length(parts) + 1L]] <- .morloc_take_records(b, width, which(ok))
      bufs[[i]] <<- .morloc_take_records(b, width, which(!ok))
    }
    .morloc_sort_records(do.call(c, unname(parts)), width)
  }
}

.morloc_new_sorter <- function(dir, width, budget) {
  s <- new.env()
  s$dir <- dir
  s$width <- width
  s$budget <- budget
  s$buf <- list()
  s$buffered <- 0
  s$runs <- list()
  s$reader <- NULL
  s$pending <- bit64::integer64(0)
  s
}

.morloc_close_runs <- function(runs) {
  for (r in runs) {
    close(r$con)
  }
}

.morloc_spill <- function(s) {
  if (s$buffered > 0) {
    words <- .morloc_sort_records(do.call(c, unname(s$buf)), s$width)
    s$runs[[length(s$runs) + 1L]] <- .morloc_write_run(s$dir, words)
    s$buf <- list()
    s$buffered <- 0
  }
  if (length(s$runs) >= .morloc_max_runs) {
    pull <- .morloc_merger(s$runs, s$width)
    path <- tempfile("morloc-sort-", tmpdir = s$dir)
    con <- file(path, open = "w+b")
    unlink(path)
    total <- 0
    repeat {
      words <- pull()
      if (length(words) == 0) break
      writeBin(unclass(words), con, size = 8L)
      total <- total + length(words)
    }
    flush(con)
    .morloc_close_runs(s$runs)
    s$runs <- list(list(con = con, words = total))
  }
}

.morloc_sorter <- function(h) {
  key <- as.character(h)
  if (!exists(key, envir = .morloc_sorters, inherits = FALSE)) {
    stop(sprintf("no open sorter %s", key))
  }
  get(key, envir = .morloc_sorters, inherits = FALSE)
}

morloc_open_sorter <- function(dir, width, kib) {
  if (width < 1 || width > 4) {
    stop(sprintf("openSorter: record width %d is not 1 to 4 words", width))
  }
  if (kib < 1) {
    stop(sprintf("openSorter: a memory budget of %d KiB is less than 1", kib))
  }
  if (!dir.exists(dir)) {
    stop(sprintf("openSorter: %s is not a directory", dir))
  }
  words <- kib * 1024 %/% 8
  .morloc_next_sorter <<- .morloc_next_sorter + 1L
  h <- .morloc_next_sorter
  budget <- max(width, words - words %% width)
  assign(as.character(h), .morloc_new_sorter(dir, as.integer(width), budget), envir = .morloc_sorters)
  h
}

morloc_push_sorter <- function(h, xs) {
  s <- .morloc_sorter(h)
  if (!is.null(s$reader)) {
    stop("pushSorter: the sorter is already being read")
  }
  if (length(xs) %% s$width != 0) {
    stop(sprintf("pushSorter: %d words are not whole records of %d", length(xs), s$width))
  }
  xs <- .morloc_as_words(xs)
  if (length(xs) == 0) {
    return(invisible(NULL))
  }
  s$buf[[length(s$buf) + 1L]] <- xs
  s$buffered <- s$buffered + length(xs)
  if (s$buffered >= s$budget) {
    .morloc_spill(s)
  }
  invisible(NULL)
}

.morloc_sorted_next <- function(s) {
  if (is.null(s$reader)) {
    if (length(s$runs) == 0) {
      words <- if (s$buffered > 0) do.call(c, unname(s$buf)) else bit64::integer64(0)
      sorted <- .morloc_sort_records(words, s$width)
      s$buf <- list()
      s$buffered <- 0
      offset <- 0
      batch <- .morloc_sorted_batch * s$width
      s$reader <- function() {
        n <- min(batch, length(sorted) - offset)
        if (n <= 0) {
          return(bit64::integer64(0))
        }
        out <- sorted[seq.int(offset + 1, length.out = n)]
        offset <<- offset + n
        out
      }
    } else {
      .morloc_spill(s)
      s$reader <- .morloc_merger(s$runs, s$width)
    }
  }
  s$reader()
}

morloc_next_sorted <- function(h) {
  tryCatch(
    list(TRUE, "", .morloc_sorted_next(.morloc_sorter(h))),
    error = function(e) list(FALSE, conditionMessage(e), bit64::integer64(0))
  )
}

morloc_close_sorter <- function(h) {
  key <- as.character(h)
  if (exists(key, envir = .morloc_sorters, inherits = FALSE)) {
    .morloc_close_runs(get(key, envir = .morloc_sorters, inherits = FALSE)$runs)
    rm(list = key, envir = .morloc_sorters)
  }
  invisible(NULL)
}
