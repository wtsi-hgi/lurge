def humanise(number):
    """Converts bytes to human-readable string."""
    if number/2**10 < 1:
        return "{}".format(number)
    elif number/2**20 < 1:
        return "{} KiB".format(round(number/2**10, 2))
    elif number/2**30 < 1:
        return "{} MiB".format(round(number/2**20, 2))
    elif number/2**40 < 1:
        return "{} GiB".format(round(number/2**30, 2))
    elif number/2**50 < 1:
        return "{} TiB".format(round(number/2**40, 2))
    else:
        return "{} PiB".format(round(number/2**50, 2))
