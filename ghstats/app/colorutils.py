import colorsys

COLORS = {
    'aqua':'#7fdbff',
    'blue':'#0074d9',
    'lime':'#01ff70',
    'navy':'#001f3f',
    'teal':'#39cccc',
    'olive':'#3d9970',
    'green':'#2ecc40',
    'red':'#ff4136',
    'maroon':'#85144b',
    'orange':'#ff851b',
    'purple':'#b10dc9',
    'yellow':'#ffdc00',
    'fuchsia':'#f012be',
    'gray':'#aaaaaa',
    'white':'#ffffff',
    'black':'#111111',
    'silver':'#dddddd',
}

def hex_to_rgb(hexstr):
    hexstr = hexstr.lstrip('#')
    rgb = []
    for i in range(0, 6, 2):
        rgb.append(int(hexstr[i:i+2], 16) / 256)
    return rgb

def hex_to_hsv(hexstr):
    rgb = hex_to_rgb(hexstr)
    return list(colorsys.rgb_to_hsv(*rgb))

def rgb_to_hex(rgb):
    rgb = [int(v * 256) for v in rgb]
    return '#{:x}{:x}{:x}'.format(*rgb)

def hsv_to_hex(hsv):
    rgb = list(colorsys.hsv_to_rgb(*hsv))
    return rgb_to_hex(rgb)

def regenerate_colors(colors, factor, increase=True):
    for key in colors.keys():
        hexstr = colors[key]
        hsv = hex_to_hsv(hexstr)
        s = hsv[1]
        if increase:
            s += factor
            if s > 1:
                s = 0.
        else:
            s -= factor
            if s < 0:
                s = 1.
        hsv[1] = s
        colors[key] = hsv_to_hex(hsv)
    return colors

def iter_colors():
    colors = COLORS.copy()
    color_iter = iter(colors.keys())
    loop_count = 0
    while True:
        try:
            key = next(color_iter)
        except StopIteration:
            increase = loop_count > 10
            colors = regenerate_colors(colors, loop_count*.1, increase)
            color_iter = iter(colors.keys())
            key = next(color_iter)
        yield colors[key]
