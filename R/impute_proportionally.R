#'  function to do proportional imputing by group
#'  @export
impute_proportionally <- function(x, y, value_name, merge_name, na_zero = FALSE) {

  if (dim(x)[2]!=3) stop("x must be on long format with 3 columns")
  if (dim(y)[2]!=3) stop("y must be on long format with 3 columns")
  if (sum(names(x) %in% names(y))!=2) stop("there must be a common group and a common value column")
  if (!(value_name %in% names(x))) stop("value_name must be in x")
  if (!(value_name %in% names(y))) stop("value_name must be in y")

  x <- as.data.table(x)
  y <- as.data.table(y)

  if ("impute_value" %in% names(x)) stop("'impute_value' is a reserved name for the function")
  if ("impute_value" %in% names(y)) stop("'impute_value' is a reserved name for the function")

  names(x)[which(names(x) == value_name)] <- "impute_value"
  names(y)[which(names(y) == value_name)] <- "impute_value"

  if ("impute_group_com" %in% names(x)) stop("'impute_group_com' is a reserved name for the function")
  if ("impute_group_com" %in% names(y)) stop("'impute_group_com' is a reserved name for the function")

  names(x)[which(names(x) == merge_name)] <- "impute_group_com"
  names(y)[which(names(y) == merge_name)] <- "impute_group_com"

  name_impute_group_x <- names(x)[which(!(names(x) %in% c("impute_value","impute_group_com")))]
  names(x)[which(!(names(x) %in% c("impute_value","impute_group_com")))] <- "impute_group_x"

  name_impute_group_y <- names(y)[which(!(names(y) %in% c("impute_value","impute_group_com")))]
  names(y)[which(!(names(y) %in% c("impute_value","impute_group_com")))] <- "impute_group_y"

  if (any(is.na(x[, impute_value])) && na_zero==FALSE) stop("NAs are not allowed in x when na_zero = FALSE")
  if (any(is.na(y[, impute_value])) && na_zero==FALSE) stop("NAs are not allowed in y when na_zero = FALSE")

  if (any(is.na(x[,impute_value])) && na_zero==TRUE) {
    warning("NAs in x are replaced by zeroes")
    x[is.na(impute_value), impute_value := 0]
  }

  if (any(is.na(y[,impute_value])) && na_zero==TRUE) {
    warning("NAs in y are replaced by zeroes")
    y[is.na(impute_value), impute_value := 0]
  }

  # check common group sums, and adust id needed
  tmpx <- x[, .(sum = sum(impute_value)),by=.(impute_group_com)]
  tmpy <- y[, .(sum = sum(impute_value)),by=.(impute_group_com)]
  tmp <- merge(tmpx, tmpy, by = "impute_group_com")

  # matplot(tmp[,-1],type="l")
  # plot(tmp[,(sum.x-sum.y)/(sum.x+sum.y)/2],type="l"); grid()

  if (NROW(tmpx)!=NROW(tmpy)) warning("not all impute_groups present, data omitted")

  if (!all(tmp$sum.x==tmp$sum.y)) {
    warning("sums are not equal in groups, values are adjusted to mean")
    tmp[, adj.sum := round((sum.x + sum.y)/2.)]
    tmp[, ':='(rel.x = adj.sum/sum.x,
               rel.y = adj.sum/sum.y)]

    x <- merge(x,tmp[,.(impute_group_com, rel.x, adj.sum)],by="impute_group_com")
    y <- merge(y,tmp[,.(impute_group_com, rel.y, adj.sum)],by="impute_group_com")

    x[, impute_value := round(impute_value * rel.x)]
    y[, impute_value := round(impute_value * rel.y)]

    x[,diff.sum := sum(impute_value) - adj.sum[1], by =.(impute_group_com)]
    x[,id.val.max := which.max(impute_value), by = .(impute_group_com)]
    x[,impute_value := impute_value - diff.sum * ((1:.N)==id.val.max), by = .(impute_group_com)]
    #x[,diff.sum := sum(impute_value) - adj.sum[1], by =.(impute_group_com)]

    y[,diff.sum := sum(impute_value) - adj.sum[1], by =.(impute_group_com)]
    y[,id.val.max := which.max(impute_value), by = .(impute_group_com)]
    y[,impute_value := impute_value - diff.sum * ((1:.N)==id.val.max), by = .(impute_group_com)]
    #y[,diff.sum := sum(impute_value) - adj.sum[1], by =.(impute_group_com)]

    x <- x[,1:3]
    y <- y[,1:3]

    # check common group sums to see if adjusting has gone ok
    tmpx <- x[, .(sum = round(sum(impute_value))),by=.(impute_group_com)]
    tmpy <- y[, .(sum = round(sum(impute_value))),by=.(impute_group_com)]
    tmp <- merge(tmpx, tmpy, by = "impute_group_com")
  }

  if (!all(tmp$sum.x==tmp$sum.y)) stop("error in adjusting group values")

  # find share imputing
  x[,impute_share := impute_value/sum(impute_value), impute_group_com]
  y[,impute_share := impute_value/sum(impute_value), impute_group_com]

  res <- merge(x, y, by="impute_group_com", allow.cartesian = TRUE)

  res[, impute_share := impute_share.x * impute_share.y]

  # include sum
  res <- merge(res,tmp[,.(impute_group_com,sum=sum.x)],by="impute_group_com")

  # get decimal number
  res[, impute_value_dec := impute_share * sum]
  res[, impute_value := floor(impute_share * sum)]

  res[, impute_value_dec := impute_value_dec - impute_value]

  # maximal number of missing values in the groups
  n.max.add <- res[,sum(impute_value_dec), by=.(impute_group_com)][, max(V1)]

  # impute by finding maximm residual in rows/cols with missing values
  for (i in 1:n.max.add) {

    res[,res.x := impute_value.x[1] - sum(impute_value), by = .(impute_group_com, impute_group_x)]
    res[,res.y := impute_value.y[1] - sum(impute_value), by = .(impute_group_com, impute_group_y)]

    add.values <- res[res.x > 0 & res.y > 0,
                      .(.I[which.max(impute_value_dec)]),
                      by = .(impute_group_com)][,V1]

    res[add.values, ':='(impute_value = impute_value + 1L,
                         impute_value_dec = impute_value_dec - 1)]

  }

  if (!res[, .(sum(impute_value), sum[1]), by= .( impute_group_com)][, all(V1==V2)]) {
    stop("imputing failed")
  }

  # clean up and rename to input names
  res <- res[,.(impute_group_com, impute_group_x, impute_group_y, impute_value)]
  names(res) <- c(merge_name, name_impute_group_x, name_impute_group_y, value_name)

  return(res)

}
