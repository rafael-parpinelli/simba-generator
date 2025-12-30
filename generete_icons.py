# generate_icons.py
from PIL import Image
import os

def gerar_icones(input_png="simba.png"):
    if not os.path.exists(input_png):
        print("❌ Arquivo simba.png não encontrado.")
        return

    img = Image.open(input_png)

    # Windows .ico
    img.save("simba.ico", sizes=[(256, 256), (128, 128), (64, 64), (32, 32), (16, 16)])
    print("✅ Ícone .ico gerado para Windows.")

    # macOS .icns (usando pillow + imagem intermediária)
    icns_dir = "simba.iconset"
    os.makedirs(icns_dir, exist_ok=True)
    for size in [16, 32, 64, 128, 256, 512]:
        img.resize((size, size)).save(f"{icns_dir}/icon_{size}x{size}.png")
        img.resize((size*2, size*2)).save(f"{icns_dir}/icon_{size}x{size}@2x.png")

    os.system(f"iconutil -c icns {icns_dir}")
    print("🍏 Ícone .icns gerado para macOS.")
    os.system(f"rm -rf {icns_dir}")

if __name__ == "__main__":
    gerar_icones()